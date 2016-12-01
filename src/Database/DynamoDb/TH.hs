{-# LANGUAGE TemplateHaskell #-}

module Database.DynamoDb.TH (
    mkTableDefs
  , deriveEncodable
  , deriveEncCollection
) where

import           Control.Lens                    (ix, over, (.~), (^.), _1)
import           Control.Monad                   (forM_)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (WriterT, execWriterT, tell)
import           Data.Bool                       (bool)
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
    tblFieldNames <- getFieldNames table
    buildColData tblFieldNames
    genBaseCollection table tblrange Nothing

    -- Instances for indices
    forM_ indexes $ \(idx, idxrange) -> do
        genBaseCollection idx idxrange (Just table)
        -- Check that all records from indices conform to main table and create instances
        instfields <- getFieldNames idx
        forM_ instfields $ \(fieldname, ltype) ->
            case lookup fieldname tblFieldNames of
                Just ptype
                  | ltype /= ptype -> fail $ "Record '" <> fieldname <> "' form index " <> show idx <> " has different type from table " <> show table
                  | otherwise -> return ()
                Nothing ->
                  fail ("Record '" <> fieldname <> "' from index " <> show idx <> " is not in present in table " <> show table)

    migfunc <- lift $ mkMigrationFunc migname table (map fst indexes)
    tell migfunc

-- | Generate basic collection instances
genBaseCollection :: Name -> Bool -> Maybe Name -> WriterT [Dec] Q ()
genBaseCollection coll collrange mparent = do
    lift [d|
      instance Generic $(pure (ConT coll))
      instance HasDatatypeInfo $(pure (ConT coll))
      |] >>= tell
    case mparent of
      Nothing ->
        lift [d|
            instance DynamoCollection $(pure (ConT coll)) $(pure (ConT $ mrange collrange)) 'IsTable
            instance DynamoTable $(pure (ConT coll)) $(pure (ConT $ mrange collrange)) 'IsTable
             |] >>= tell
      Just parent ->
        lift [d|
            instance DynamoCollection $(pure (ConT coll)) $(pure (ConT $ mrange collrange)) 'IsIndex
            instance DynamoIndex $(pure (ConT coll)) $(pure (ConT parent)) $(pure (ConT $ mrange collrange)) 'IsIndex
              |] >>= tell

    tblFieldNames <- getFieldNames coll
    -- Skip primary key, we cannot filter by it
    let constrNames = mkConstrNames tblFieldNames
    forM_ (drop (bool 1 2 collrange) constrNames) $ \constr ->
      lift [d|
        instance InCollection $(pure (ConT constr)) $(pure (ConT coll)) 'OuterQuery
        |] >>= tell
  where
    mrange True = 'WithRange
    mrange False = 'NoRange


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
buildColData :: [(String, Type)] -> WriterT [Dec] Q ()
buildColData fieldlist = do
    let constrNames = mkConstrNames fieldlist
    forM_ (zip fieldlist constrNames) $ \((fieldname, ltype), constr) -> do
        let pat = mkName (toPatName fieldname)
        say $ DataD [] constr [] [] []
        lift [d|
            instance ColumnInfo $(pure (ConT constr)) where
                columnName _ = T.pack fieldname
          |] >>= tell
        say $ SigD pat (AppT (AppT (AppT (ConT ''Column) ltype) (ConT 'TypColumn)) (ConT constr))
        say $ ValD (VarP pat) (NormalB (ConE 'Column)) []
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
    buildColData tblFieldNames
    -- Create instance DynamoEncodable
    deriveEncodable table

-- | Derive just the DynamoEncodable instance
-- for structures that already have DynamoTable/DynamoIndex and you want to use
-- them inside other records
--
-- Creates:
-- >>> instance DynamoEncodable Type where
-- >>>   dEncode val = Just (attributeValue & avM .~ gdEncode val)
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
        instance InCollection $(pure (ConT constr)) $(pure (ConT table)) 'InnerQuery
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
