{-# LANGUAGE TemplateHaskell #-}

-- | Template Haskell macros to automatically derive instances, create column datatypes
--  and create migrations functions.
--
module Database.DynamoDb.TH (
    -- * Derive instances for table and indexes
    -- $table

    -- * Derive instances for nested records
    -- $nested

    -- * Macros
    mkTableDefs
  , deriveCollection
  , deriveEncodable
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
import           Network.AWS                     (MonadAWS)

import           Database.DynamoDb.Class
import           Database.DynamoDb.Migration     (runMigration)
import           Database.DynamoDb.Types
import           Database.DynamoDb.Internal

-- | Create instances, datatypes for table, fields and instances.
--
-- Example of what gets created:
--
-- > data Test { first :: Text, second :: Text, third :: Int } deriving (GHC.Generic)
-- > data TestIndex { third :: Int, second :: T.Text} deriving (GHC.Generic)
-- >
-- > $(mkTableDefs (''Test, True) [(''TestIndex, False)] )
-- > instance Generic Test
-- > instance HasDatatypeInfo Test
-- > instance DynamoCollection Test WithRange IsTable
-- > instance DynamoTable Test WithRange IsTable
-- >
-- > instance Generic TestIndex
-- > instance HasDatatypeInfo TestIndex
-- > instance DynamoCollection TestIndex NoRange IsIndex
-- > instance DynamoIndex TestIndex Test NoRange IsIndex
-- >
-- > data P_First
-- > instance InCollection P_First Test 'NestedPath
-- > data P_Second
-- > instance InCollection P_Second Test 'NestedPath
-- > instance InCollection P_Second TestIndex 'NestedPath
-- > instance InCollection P_Second TestIndex 'FullPath
-- > data P_Third
-- > instance InCollection P_Third Test 'NestedPath
-- > instance InCollection P_Third Test 'FullPath
-- > instance InCollection P_Third TestIndex 'NestedPath
-- > instance InCollection P_Third TestIndex 'FullPath
-- >
-- > colFirst :: Column Text TypColumn P_First
-- > colFirst = Column
-- > colSecond :: Column Text TypColumn P_Second
-- > colSecond = Column
-- > colThird :: Column Int TypColumn P_Third
-- > colThidr = Column
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
        instance InCollection $(pure (ConT constr)) $(pure (ConT coll)) 'FullPath
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
        say $ ValD (VarP pat) (NormalB (VarE 'mkColumn)) []
  where
    toPatName = ("col" <> ) . over (ix 0) toUpper

say :: Monad m => t -> WriterT [t] m ()
say a = tell [a]

-- | Derive 'DynamoEncodable' and prepare column instances for nested structures.
deriveCollection :: Name -> Q [Dec]
deriveCollection table =
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

-- | Derive just the 'DynamoEncodable' instance
-- for structures that were already derived using 'mkTableDefs'
-- and you want to use them as nested structures as well.
--
-- Creates:
--
-- > instance DynamoEncodable Type where
-- >   dEncode val = Just (attributeValue & avM .~ gdEncode val)
-- >   dDecode (Just attr) = gdDecode (attr ^. avM)
-- >   dDecode Nothing = Nothing
-- > instance InCollection column_type P_Column1 'NestedPath
-- > instance InCollection column_type P_Column2 'NestedPath
-- > ...
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
        instance InCollection $(pure (ConT constr)) $(pure (ConT table)) 'NestedPath
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

-- $table
--
-- Use 'mkTableDefs' to derive everything about a table and its indexes. After running the function,
-- you will end up with lots of instances, data types for columns ('P_TId', 'P_TBase', 'P_TDescr')
-- and smart constructors for column ('colTId', 'colTBase', 'colTDescr', etc.) and one function (migrate)
-- that creates table and updates the indexes.
--
-- * Table is named after a constructor of the datatype (Test)
-- * Attribute name is a field name from a first underscore ('tId'). This should make it compatibile with lens.
--   Underscore is not mandatory.
-- * Column name is capitalized attribute name with prepended 'col' ('colTId')
-- * Attribute names in an index table must be the same as Attribute names in the main table
-- * Auxiliary datatype for column is P_ followed by capitalized attribute name ('P_TId')
--
-- @
-- data Test = Test {
--     _tId :: Int
--   , _tBase :: T.Text
--   , _tDescr :: T.Text
--   , _tDate :: T.Text
--   , _tDict :: HashMap T.Text Inner
-- } deriving (Show, GHC.Generic)
--
-- data TestIndex = TestIndex {
--   , i_tDate :: T.Text
--   , i_tDescr :: T.Text
-- } deriving (Show, GHC.Generic)
-- $(mkTableDefs "migrate" (''Test, True) [(''TestIndex, False)])
-- @
--

-- $nested
--
-- Use 'deriveCollection' for records that are nested. Use 'deriveEncodable' for records that are
-- nested in one table and serve as its own table at the same time.
--
-- @
-- data Book = Book {
--      author :: T.Text
--    , title :: T.Text
-- }
-- $(deriveCollection ''Book)
--
-- data Test = Test {
--     _tId :: Int
--   , _tBase :: T.Text
--   , _tBooks :: [Book]
-- } deriving (Show, GHC.Generic)
-- $(mkTableDefs "migrate" (''Test, True) [])
-- @
