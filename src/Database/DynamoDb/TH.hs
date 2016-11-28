{-# LANGUAGE TemplateHaskell #-}
module Database.DynamoDb.TH (
    mkTableDefs
  , deriveEncodable
  , deriveEncCollection
) where

import           Control.Lens                    (ix, over, _1, (.~), (^.))
import           Control.Monad                   (forM_, void)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (execWriterT, tell, WriterT)
import           Data.Char                       (toUpper)
import           Data.Monoid                     ((<>))
import qualified Data.Text                       as T
import           Generics.SOP
import           Language.Haskell.TH
import           Language.Haskell.TH.Syntax      (Name (..), OccName (..))
import           Network.AWS.DynamoDB.Types     (attributeValue, avM)
import Data.Function ((&))

import           Database.DynamoDb.Class
import           Database.DynamoDb.Filter
import           Database.DynamoDb.Types

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
    constrNames <- buildColData table tblFieldNames

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
    mrange True = ''WithRange
    mrange False = ''NoRange


getFieldNames :: Name -> WriterT [Dec] Q [(String, Type)]
getFieldNames tbl = do
    info <- lift $ reify tbl
    case getRecords info of
      Left err -> fail $ "Table " <> show tbl <> ": " <> err
      Right lst -> return $ map (over _1 (T.unpack . translateFieldName)) lst
  where
    getRecords :: Info -> Either String [(String, Type)]
    getRecords (TyConI (DataD _ _ _ [RecC _ vars] _)) = Right $ map (\(Name (OccName rname) _,_,typ) -> (rname, typ)) vars
    getRecords _ = Left "not a record declaration with 1 constructor"

toConstrName :: String -> String
toConstrName = ("P_" <>) . over (ix 0) toUpper

buildColData :: Name -> [(String, Type)] -> WriterT [Dec] Q [Name]
buildColData table fieldlist = do
    constrNames <- lift $ mapM (newName . toConstrName . fst) fieldlist
    forM_ (zip fieldlist constrNames) $ \((fieldname, ltype), constr) -> do
        let pat = mkName (toPatName fieldname)
        say $ DataD [] constr [] [] []
        say $ InstanceD [] (AppT (AppT (ConT ''InCollection) (ConT constr)) (ConT table)) []
        say $ InstanceD [] (AppT (ConT ''ColumnInfo) (ConT constr)) [FunD 'columnName [Clause [WildP] (NormalB (LitE (StringL fieldname))) []]]
        say $ SigD pat (AppT (AppT (AppT (ConT ''Column) ltype) (ConT ''TypColumn)) (ConT constr))
        say $ ValD (VarP pat) (NormalB (ConE 'Column)) []
    return constrNames
  where
    toPatName = ("col" <> ) . over (ix 0) toUpper

say :: Monad m => t -> WriterT [t] m ()
say a = tell [a]

-- | Derive DynamoEncodable and prepare column instances for inner structures
deriveEncCollection :: Name -> Q [Dec]
deriveEncCollection table =
  execWriterT $ do
    say $ InstanceD [] (AppT (ConT ''Generic) (ConT table)) []
    say $ InstanceD [] (AppT (ConT ''HasDatatypeInfo) (ConT table)) []
    -- Create instance DynamoEncodable
    enc <- lift $ deriveEncodable table
    tell enc
    -- Create column data
    tblFieldNames <- getFieldNames table
    void $ buildColData table tblFieldNames

-- | Derive just the DynamoEncodable instance
-- for structures that already have DynamoTable/DynamoIndex and you want to use
-- them inside other records
deriveEncodable :: Name -> Q [Dec]
deriveEncodable table =
  execWriterT $ do
    attr1 <- lift $ newName "attr"
    tbl1 <- lift $ newName "tbl"
    say $ InstanceD [] (AppT (ConT ''DynamoEncodable) (ConT table))
      [FunD 'dEncode [Clause [VarP tbl1]
        (NormalB (InfixE (Just (VarE 'attributeValue))
            (VarE '(Data.Function.&)) (Just (InfixE (Just (VarE 'avM))
              (VarE '(.~)) (Just (AppE (VarE 'gdEncode) (VarE tbl1))))))) []],
              FunD 'dDecode [Clause [ConP 'Just [VarP attr1]]
              (NormalB (AppE (VarE 'gdDecode) (InfixE (Just (VarE attr1))
               (VarE '(^.)) (Just (VarE 'avM))))) [],
               Clause [ConP 'Nothing []] (NormalB (ConE 'Nothing)) []]]
