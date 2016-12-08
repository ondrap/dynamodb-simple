{-# LANGUAGE CPP             #-}
{-# LANGUAGE TemplateHaskell #-}

-- | Create polymorphic lens to access table & indexes
module Database.DynamoDB.THLens where

import           Control.Lens                    (over, _1)
import           Control.Monad                   (forM_)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (WriterT, tell)
import           Data.Function                   ((&))
import           Data.List                       (isPrefixOf)
import           Data.Monoid                     ((<>))
import           Language.Haskell.TH
import           Language.Haskell.TH.Syntax      (Name (..), OccName (..))

-- | Create lenses if the field in the primary table starts with _.
--
-- class Test_lens_field00 a b | a -> b where
--   _treti :: Functor f => (b -> f b) -> a -> f a
-- instance Test_lens_field00 Test (Maybe T.Text) where
--   field f t = (\txt -> t{_field=txt}) <$> f (_field t)
createPolyLenses :: (String -> String) -> Name -> [Name] -> WriterT [Dec] Q ()
createPolyLenses translate table indexes = do
    -- Get fields that can be lenses
    tblfields <- getFieldNames table id
    fields <- tblfields & map fst
                        & filter ("_" `isPrefixOf`)
                        & map ((,) <$> translate <*> drop 1)
                        & mapM mkLensClass
    mapM_ createClass fields
    createInstances fields table
    mapM_ (createInstances fields) indexes
  where
    mkLensClass (field, lens) = do
        clsname <- lift $ newName (nameBase table ++ "_lens_" ++ field)
        return (field, (mkName lens, clsname))
    createClass (_, (lensname, clsname)) = do
        a <- lift $ newName "a"
        b <- lift $ newName "b"
        f <- lift $ newName "f"
        lift (pure $ ClassD [] clsname [PlainTV a,PlainTV b] [FunDep [a] [b]]
            [SigD lensname (ForallT [PlainTV f] [AppT (ConT ''Functor) (VarT f)]
              (AppT (AppT ArrowT (AppT (AppT ArrowT (VarT b))
              (AppT (VarT f) (VarT b)))) (AppT (AppT ArrowT (VarT a))
              (AppT (VarT f) (VarT a)))))]) >>= say
    createInstances lensfields idx = do
        tblfields <- getFieldNames idx id
        forM_ tblfields $ \(fieldname, ftype) ->
            whenJust (lookup (translate fieldname) lensfields) $ \(lensname, clsname) -> do
                f <- lift $ newName "f"
                t <- lift $ newName "t"
                val <- lift $ newName "val"
                let fieldSel = mkName fieldname
                lift (pure $ InstanceD [] (AppT (AppT (ConT clsname) (ConT idx)) ftype)
                  [FunD lensname [Clause [VarP f,VarP t] (NormalB (InfixE (Just (LamE [VarP val]
                      (RecUpdE (VarE t) [(fieldSel,VarE val)]))) (VarE 'fmap)
                                  (Just (AppE (VarE f) (AppE (VarE fieldSel) (VarE t)))))) []]])
                    >>= say

whenJust :: Monad m => Maybe a -> (a -> m ()) -> m ()
whenJust (Just a) f = f a
whenJust Nothing _ = return ()

say :: Monad m => t -> WriterT [t] m ()
say a = tell [a]


-- | Reify name and return list of record fields with type
getFieldNames :: Name -> (String -> String) -> WriterT [Dec] Q [(String, Type)]
getFieldNames tbl translate = do
    info <- lift $ reify tbl
    case getRecords info of
      Left err -> fail $ "Table " <> show tbl <> ": " <> err
      Right lst -> return $ map (over _1 translate) lst
  where
    getRecords :: Info -> Either String [(String, Type)]
#if __GLASGOW_HASKELL__ >= 800
    getRecords (TyConI (DataD _ _ _ _ [RecC _ vars] _)) = Right $ map (\(Name (OccName rname) _,_,typ) -> (rname, typ)) vars
#else
    getRecords (TyConI (DataD _ _ _ [RecC _ vars] _)) = Right $ map (\(Name (OccName rname) _,_,typ) -> (rname, typ)) vars
#endif
    getRecords _ = Left "not a record declaration with 1 constructor"
