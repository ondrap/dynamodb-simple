{-# LANGUAGE TemplateHaskell #-}

module Database.DynamoDb.TH (
    mkTableDefs
  , deriveEncodable
  , deriveEncCollection
) where

import           Control.Lens                    (ix, over, (.~), (^.), _1)
import           Control.Monad                   (forM_, void)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (WriterT, execWriterT, tell)
import           Data.Char                       (toUpper)
import           Data.Function                   ((&))
import           Data.Monoid                     ((<>))
import qualified Data.Text                       as T
import           Generics.SOP
import           Language.Haskell.TH
import           Language.Haskell.TH.Syntax      (Name (..), OccName (..))
import           Network.AWS.DynamoDB.Types      (attributeValue, avM)
import           Network.AWS (MonadAWS)

import           Database.DynamoDb.Class
import           Database.DynamoDb.Filter
import           Database.DynamoDb.Migration     (runMigration)
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
    String -- ^ Name of the migration function
  -> (Name, Bool)      -- ^ Main record type name, bool indicates if it has a sort key
  -> [(Name, Bool)] -- ^ Index records, bool indicates if it has a sort key
  -> Q [Dec]
mkTableDefs migname (table, tblrange) indexes =
  execWriterT $ do
    -- Instances for main table
    lift [d|
      instance Generic $(pure (ConT table))
      instance HasDatatypeInfo $(pure (ConT table))
      instance DynamoCollection $(pure (ConT table)) $(pure (ConT $ mrange tblrange)) IsTable
      instance DynamoTable $(pure (ConT table)) $(pure (ConT $ mrange tblrange)) IsTable
      |] >>= tell
    --
    tblFieldNames <- getFieldNames table
    constrNames <- buildColData tblFieldNames
    forM_ constrNames $ \constr ->
      lift [d|
        instance InCollection $(pure (ConT constr)) $(pure (ConT table))
        |] >>= tell

    let tableAssoc = zip (map fst tblFieldNames) (zip constrNames (map snd tblFieldNames))
    -- Instances for indices
    forM_ indexes $ \(idx, idxrange) -> do
        lift [d|
          instance Generic $(pure (ConT idx))
          instance HasDatatypeInfo $(pure (ConT idx))
          instance DynamoCollection $(pure (ConT idx)) $(pure (ConT $ mrange idxrange)) IsIndex
          instance DynamoIndex $(pure (ConT idx)) $(pure (ConT table)) $(pure (ConT $ mrange idxrange)) IsIndex
          |] >>= tell

        -- Check that all records from indices conform to main table and create instances
        instfields <- getFieldNames idx
        forM_ instfields $ \(fieldname, ltype) ->
            case lookup fieldname tableAssoc of
                Just (constr, ptype)
                  | ltype == ptype -> say $ InstanceD [] (AppT (AppT (ConT ''InCollection) (ConT constr)) (ConT idx)) []
                  | otherwise -> fail $ "Record '" <> fieldname <> "' form index " <> show idx <> " has different type from table " <> show table
                Nothing ->
                  fail ("Record '" <> fieldname <> "' from index " <> show idx <> " is not in present in table " <> show table)
    migfunc <- lift $ mkMigrationFunc migname table (map fst indexes)
    tell migfunc
  where
    mrange True = ''WithRange
    mrange False = ''NoRange

-- | Reify name and return list of record fields with type
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

mkConstrNames :: [(String,a)] -> [Name]
mkConstrNames = map (mkName . toConstrName . fst)

-- | Build P_Column0 data, add it to instances and make colColumn variable
buildColData :: [(String, Type)] -> WriterT [Dec] Q [Name]
buildColData fieldlist = do
    let constrNames = mkConstrNames fieldlist
    forM_ (zip fieldlist constrNames) $ \((fieldname, ltype), constr) -> do
        let pat = mkName (toPatName fieldname)
        say $ DataD [] constr [] [] []
        lift [d|
            instance ColumnInfo $(pure (ConT constr)) where
                columnName _ = T.pack fieldname
          |] >>= tell
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
    lift [d|
      instance Generic $(pure (ConT table))
      instance HasDatatypeInfo $(pure (ConT table))
      |] >>= tell
    -- Create column data
    tblFieldNames <- getFieldNames table
    void $ buildColData tblFieldNames
    -- Create instance DynamoEncodable
    deriveEncodable table

-- | Derive just the DynamoEncodable instance
-- for structures that already have DynamoTable/DynamoIndex and you want to use
-- them inside other records
--
-- Creates:
-- >>> instance DynamoEncodable Type where
-- >>>   dEncode val = attributeValue & avM .~ gdEncode val
-- >>>   dDecode (Just attr) = gdDecode (attr ^. avM)
-- >>>   dDecode Nothing = Nothing
deriveEncodable :: Name -> WriterT [Dec] Q ()
deriveEncodable table = do
    lift [d|
      instance DynamoEncodable $(pure (ConT table)) where
          dEncode val = Just (attributeValue & avM .~ gdEncode val)
          dDecode (Just attr) = gdDecode (attr ^. avM)
          dDecode Nothing = Nothing
      |] >>= tell
    tblFieldNames <- getFieldNames table
    let constrs = mkConstrNames tblFieldNames
    forM_ constrs $ \constr ->
      lift [d|
        instance InCollection $(pure (ConT constr)) $(pure (ConT table))
        |] >>= tell

-- | Creates top-leval variable as a call to a migration function with partially applied createIndex
mkMigrationFunc :: String -> Name -> [Name] -> Q [Dec]
mkMigrationFunc name table indices = do
    let lstmap = ListE (map idxtemplate indices)
    let funcname = mkName name
    m <- newName "m"
    let signature = SigD funcname (ForallT [PlainTV m] [AppT (ConT ''MonadAWS) (VarT m)] (AppT (VarT m) (TupleT 0)))
    return [signature, ValD (VarP funcname) (NormalB (AppE (AppE (VarE 'runMigration)
              (SigE (ConE 'Proxy) (AppT (ConT ''Proxy)
              (ConT table)))) lstmap)) []]
  where
    idxtemplate idx = AppE (VarE 'createIndex) (SigE (ConE 'Proxy) (AppT (ConT ''Proxy) (ConT idx)))
