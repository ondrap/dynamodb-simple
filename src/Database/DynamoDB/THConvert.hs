{-# LANGUAGE CPP             #-}
{-# LANGUAGE TemplateHaskell #-}
-- | Module generating 'toTable' class/instance for converting from index to main table type
module Database.DynamoDB.THConvert (
  createTableConversions
) where

import           Control.Monad                   (forM_, replicateM, when)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (WriterT)
import           Data.List                       (elemIndex, sort)
import           Data.Monoid                     ((<>))
import           Language.Haskell.TH

import           Database.DynamoDB.THLens        (getFieldNames, say)


getConstructor :: Name -> Q Name
getConstructor tbl = do
    info <- reify tbl
    case info of
#if __GLASGOW_HASKELL__ >= 800
        (TyConI (DataD _ _ _ _ [RecC name _] _)) -> return name
#else
        (TyConI (DataD _ _ _ [RecC name _] _)) -> return name
#endif
        _ -> fail "not a record declaration with 1 constructor"


createTableConversions :: (String -> String) -> Name -> [Name] -> WriterT [Dec] Q ()
createTableConversions translate table idxes = do
  tblFields <- getFieldNames table translate
  let tblNames = map fst tblFields
  tblConstr <- lift $ getConstructor table
  clsname <- lift $ newName "IndexToTable"
  a <- lift $ newName "a"
  let funcname = mkName ("to" <> nameBase table)
  say $ ClassD [] clsname [PlainTV a] []
          [SigD funcname (AppT (AppT ArrowT (VarT a)) (ConT table))]
  forM_ idxes $ \idxname -> do
      idxFields <- getFieldNames idxname translate
      let idxNames = map fst idxFields
      idxConstr <- lift $ getConstructor idxname

      when (sort tblNames == sort idxNames) $
        case mapM (`elemIndex` idxNames) tblNames of
            Nothing -> return ()
            Just varidxmap -> do
                varnames <- lift $ replicateM (length idxNames) (newName "a")
                let ivars = map varP varnames
                let toJust = zipWith makeJust (map snd tblFields) (map (snd . (idxFields !!)) varidxmap)
                    olist = zipWith ($) toJust $ map (varnames !!) varidxmap
                    ovars = foldl appE (conE tblConstr) olist
                let func = funD funcname [clause [conP idxConstr ivars] (normalB ovars) []]
#if __GLASGOW_HASKELL__ >= 800
                lift (instanceD Nothing (pure []) (appT (conT clsname) (conT idxname))
                      [func]) >>= say
#else
                lift (instanceD (pure []) (appT (conT clsname) (conT idxname))
                      [func]) >>= say
#endif
  where
    makeJust (AppT (ConT mbtype) dsttype) srctype
        | mbtype == ''Maybe && dsttype == srctype = appE (conE 'Just) . varE
    makeJust _ _ = varE
