{-# LANGUAGE TemplateHaskell #-}
module Database.DynamoDb.TH (
  mkTableDefs
) where

import           Control.Lens                    (ix, over, _1)
import           Control.Monad                   (forM_)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (execWriterT, tell)
import           Data.Char                       (toUpper)
import           Data.Monoid                     ((<>))
import qualified Data.Text                       as T
import           Generics.SOP
import           Language.Haskell.TH
import           Language.Haskell.TH.Syntax      (Name (..), OccName (..))

import           Database.DynamoDb.Class
import           Database.DynamoDb.Filter

getRecords :: Info -> Either String [(String, Type)]
getRecords (TyConI (DataD _ _ _ [RecC _ vars] _)) = Right $ map (\(Name (OccName rname) _,_,typ) -> (rname, typ)) vars
getRecords _ = Left "not a record declaration with 1 constructor"

-- | Create instances, datatypes for table, fields and instances
--
-- Example of what gets created:
-- >>> data Test { first :: Text, second :: Text, third :: Int }
-- >>> data TestIndex { third :: Int, second :: T.Text}
--
-- >>> $(mkTableDefs (''Test, True) [(''TestIndex, False)] )
-- >>> instance Generic Test
-- >>> instance HasDatatypeInfo Test
-- >>> instance DynamoCollection Test WithRange IsTable
-- >>> instance DynamoTable Test WithRange IsTable
--
-- >>> instance Generic TestIndex
-- >>> instance HasDatatypeInfo TestIndex
-- >>> instance DynamoCollection TestIndex NoRange IsIndex
-- >>> instance DynamoIndex TestIndex Test NoRange IsIndex
--
-- >>> data P_First0
-- >>> instance InCollection P_First0 Test
-- >>> data P_Second0
-- >>> instance InCollection P_Second0 Test
-- >>> instance InCollection P_Second0 TestIndex
-- >>> data P_Third0
-- >>> instance InCollection P_Third0 Test
-- >>> instance InCollection P_Third0 TestIndex
--
-- >> colFirst :: Column Text TypColumn P_First0
-- >> colFirst = Column
-- >> colSecond :: Column Text TypColumn P_Second0
-- >> colSecond = Column
-- >> colThird :: Column Int TypColumn P_Third0
-- >> colThidr = Column
mkTableDefs ::
  (Name, Bool)      -- ^ Main record type name, bool indicates if it has a sort key
  -> [(Name, Bool)] -- ^ Index records, bool indicates if it has a sort key
  -> Q [Dec]
mkTableDefs (table, tblrange) indexes =
  execWriterT $ do
    -- Instances for main table
    say $ InstanceD [] (AppT (ConT ''Generic) (ConT table)) []
    say $ InstanceD [] (AppT (ConT ''HasDatatypeInfo) (ConT table)) []
    say $ InstanceD [] (AppT (AppT (AppT (ConT ''DynamoCollection) (ConT table)) (ConT (mrange tblrange))) (ConT ''IsTable)) []
    say $ InstanceD [] (AppT (AppT (AppT (ConT ''DynamoTable) (ConT table)) (ConT (mrange tblrange))) (ConT ''IsTable)) []
    --
    tblFieldNames <- getFieldNames table
    constrNames <- lift $ mapM (newName . toConstrName . fst) tblFieldNames
    let patNames = map (mkName . toPatName . fst) tblFieldNames
    forM_ (zip3 tblFieldNames constrNames patNames) $ \((fieldname, ltype), constr, pat) -> do
        say $ DataD [] constr [] [] []
        say $ InstanceD [] (AppT (AppT (ConT ''InCollection) (ConT constr)) (ConT table)) []
        say $ InstanceD [] (AppT (ConT ''ColumnInfo) (ConT constr)) [FunD 'columnName [Clause [WildP] (NormalB (LitE (StringL fieldname))) []]]
        say $ SigD pat (AppT (AppT (AppT (ConT ''Column) ltype) (ConT ''TypColumn)) (ConT constr))
        say $ ValD (VarP pat) (NormalB (ConE 'Column)) []

    let tableAssoc = zip (map fst tblFieldNames) (zip constrNames (map snd tblFieldNames))

    -- Instances for indices
    forM_ indexes $ \(idx, idxrange) -> do
        say $ InstanceD [] (AppT (ConT ''Generic) (ConT idx)) []
        say $ InstanceD [] (AppT (ConT ''HasDatatypeInfo) (ConT idx)) []
        say $ InstanceD [] (AppT (AppT (AppT (ConT ''DynamoCollection) (ConT idx)) (ConT (mrange idxrange))) (ConT ''IsIndex)) []
        say $ InstanceD [] (AppT (AppT (AppT (AppT (ConT ''DynamoIndex) (ConT idx)) (ConT table)) (ConT (mrange idxrange))) (ConT ''IsIndex)) []

        -- Check that all records from indices conform to main table and create instances
        instfields <- getFieldNames idx
        forM_ instfields $ \(fieldname, ltype) ->
            case lookup fieldname tableAssoc of
                Just (constr, ptype)
                  | ltype == ptype -> say $ InstanceD [] (AppT (AppT (ConT ''InCollection) (ConT constr)) (ConT idx)) []
                  | otherwise -> fail $ "Record '" <> fieldname <> "' form index " <> show idx <> " has different type from table " <> show table
                Nothing ->
                  fail ("Record '" <> fieldname <> "' from index " <> show idx <> " is not in present in table " <> show table)
  where
    toConstrName = ("P_" <>) . over (ix 0) toUpper
    toPatName = ("col" <> ) . over (ix 0) toUpper
    say a = tell [a]
    mrange True = ''WithRange
    mrange False = ''NoRange

    getFieldNames tbl = do
        info <- lift $ reify tbl
        case getRecords info of
          Left err -> fail $ "Table " <> show tbl <> ": " <> err
          Right lst -> return $ map (over _1 (T.unpack . translateFieldName)) lst
