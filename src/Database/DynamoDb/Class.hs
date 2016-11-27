{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE DefaultSignatures          #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE PolyKinds          #-}
{-# LANGUAGE EmptyDataDecls          #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE UndecidableInstances          #-}

module Database.DynamoDb.Class (
    DynamoCollection(..)
  , DynamoTable(..)
  , DynamoIndex(..)
  , queryKey, queryKeyRange
  , NoRange, WithRange
  , IsTable, IsIndex
  , gdDecode
) where

import           Control.Lens                     ((.~))
import           Data.Foldable                    (toList)
import           Data.Function                    ((&))
import qualified Data.HashMap.Strict              as HMap
import           Data.List.NonEmpty               (nonEmpty, NonEmpty((:|)))
import           Data.Monoid                      ((<>))
import qualified Data.Text                        as T
import           Generics.SOP
import           GHC.Exts                         (Constraint)
import qualified Network.AWS.DynamoDB.CreateTable as D
import qualified Network.AWS.DynamoDB.DeleteItem  as D
import qualified Network.AWS.DynamoDB.DeleteTable  as D
import qualified Network.AWS.DynamoDB.Query  as D
import qualified Network.AWS.DynamoDB.PutItem     as D
import           Network.AWS.DynamoDB.Types       (AttributeValue,
                                                   ProvisionedThroughput, keySchemaElement,
                                                   globalSecondaryIndex)
import qualified Network.AWS.DynamoDB.Types       as D

import Database.DynamoDb.Types

-- | Data type for collections with only hash key
data NoRange
-- | Data type for collections with hash key and sort key
data WithRange

-- | Helper type to distinguish index and table collections
data IsTable
-- | Helper type to distinguish index and table collections
data IsIndex
-- | Basic instance for dynamo collection (table or index)
-- This instances fixes the tableName and the sort key
class (Generic a, HasDatatypeInfo a, PrimaryFieldCount r) => DynamoCollection a r t | a -> r, a -> t where
  primaryFields :: (Code a ~ '[ xs ': rest ]) => Proxy a -> NonEmpty T.Text
  primaryFields _ = key :| take (primaryFieldCount (Proxy :: Proxy r) - 1) rest
    where
      (key :| rest) = gdFieldNames (Proxy :: Proxy a)

class PrimaryFieldCount r where
  primaryFieldCount :: Proxy r -> Int
instance PrimaryFieldCount NoRange where
  primaryFieldCount _ = 1
instance PrimaryFieldCount WithRange where
  primaryFieldCount _ = 2

-- | Descritpion of dynamo table
class (DynamoCollection a r t, Generic a, HasDatatypeInfo a) => DynamoTable a r t | a -> r where
  -- | Dynamo table/index name; default is the constructor name
  tableName :: Proxy a -> T.Text
  default tableName :: (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

  -- | Serialize data, put it into the database
  putItem :: a -> D.PutItem
  default putItem :: (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a)) => a -> D.PutItem
  putItem = defaultPutItem

  -- |
  createTable :: Proxy a -> ProvisionedThroughput -> D.CreateTable
  default createTable :: (TableCreate a r, Generic a, HasDatatypeInfo a, RecordOK (Code a) r,
                          Code a ~ '[ hash ': range ': rest ])
                           => Proxy a -> ProvisionedThroughput -> D.CreateTable
  createTable = iCreateTable (Proxy :: Proxy r)

  deleteTable :: Proxy a -> D.DeleteTable
  deleteTable p = D.deleteTable (tableName p)

  deleteItem :: (DeleteItem a r, Code a ~ '[ key ': hash ': rest ], RecordOK (Code a) r)
      => Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
  deleteItem = iDeleteItem (Proxy :: Proxy r)

-- | Dispatch class for NoRange/WithRange deleteItem
class DeleteItem a r where
  iDeleteItem :: (DynamoTable a r t, RecordOK (Code a) r, Code a ~ '[ hash ': range ': xss ])
            => Proxy r -> Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
instance DeleteItem a NoRange where
  iDeleteItem _ p key =
        D.deleteItem (tableName p) & D.diKey .~ HMap.singleton (fst $ gdHashField p) (dEncode key)
instance DeleteItem a WithRange where
  iDeleteItem _ p (key, range) = D.deleteItem (tableName p) & D.diKey .~ HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dEncode key), (fst $ gdRangeField p, dEncode range)]

-- | Dispatch class for NoRange/WithRange createTable
class TableCreate a r where
  iCreateTable :: (DynamoTable a r t, RecordOK (Code a) r, Code a ~ '[ hash ': range ': xss ])
                           => Proxy r -> Proxy a -> ProvisionedThroughput -> D.CreateTable
instance TableCreate a NoRange where
  iCreateTable _ = defaultCreateTable
instance TableCreate a WithRange where
  iCreateTable _ = defaultCreateTableRange

-- | Instance for tables that can be queried
class DynamoCollection a r t => TableQuery a r t where
  type QueryRange (b :: [[k]]) r :: *
  -- | Return table name and index name
  qTableName :: Proxy a -> T.Text
  qIndexName :: Proxy a -> Maybe T.Text
  -- | Create a query only by hash key
  queryKey :: (Code a ~ '[ key ': rest ], DynamoScalar key) => Proxy a -> key -> D.Query
  queryKey = defaultQueryKey
  -- | Create a query using both hash key and operation on range key
  -- On tables without range key, this degrades to queryKey
  queryKeyRange ::
        (TableQuery a r t, Code a ~ '[ key ': hash ': rest ], RecordOK (Code a) r)
      => Proxy a -> QueryRange (Code a) r -> D.Query

instance (DynamoTable a NoRange IsTable, Code a ~ '[ xs ]) => TableQuery a NoRange IsTable where
  type QueryRange ('[ key ': rest ]) NoRange  = key
  queryKeyRange = defaultQueryKey
  qTableName = tableName
  qIndexName _ = Nothing
instance  (DynamoTable a WithRange IsTable, Code a ~ '[ xs ]) => TableQuery a WithRange IsTable where
  type QueryRange ('[ key ': range ': rest ] ) WithRange  = (key, RangeOper range)
  queryKeyRange = defaultQueryKeyRange
  qTableName = tableName
  qIndexName _ = Nothing
instance (DynamoIndex a parent NoRange IsIndex, DynamoTable parent r1 t1, DynamoCollection a NoRange IsIndex, Code a ~ '[ xs ]) => TableQuery a NoRange IsIndex where
  type QueryRange ('[ key ': rest ]) NoRange  = key
  queryKeyRange = defaultQueryKey
  qTableName _ = tableName (Proxy :: Proxy parent)
  qIndexName = Just . indexName
instance  (DynamoIndex a parent WithRange IsIndex, DynamoTable parent r1 t1, DynamoCollection a WithRange IsIndex, Code a ~ '[ xs ]) => TableQuery a WithRange IsIndex where
  type QueryRange ('[ key ': range ': rest ] ) WithRange  = (key, RangeOper range)
  queryKeyRange = defaultQueryKeyRange
  qTableName _ = tableName (Proxy :: Proxy parent)
  qIndexName = Just . indexName

-- | Parameter type for queryKeyRange
type family PrimaryKey (a :: [[k]]) r :: *
type instance PrimaryKey ('[ key ': range ': rest ] ) WithRange = (key, range)
type instance PrimaryKey ('[ key ': rest ]) NoRange = key

-- | Constraint to check that hash/sort key are scalar
type family RecordOK (a :: [[k]]) r :: Constraint
type instance RecordOK '[ hash ': rest ] NoRange = (DynamoScalar hash)
type instance RecordOK '[ hash ': range ': rest ] WithRange = (DynamoScalar hash, DynamoScalar range)

-- | Class representing a Global Secondary Index
class (DynamoCollection a r t, Generic a, HasDatatypeInfo a) => DynamoIndex a parent r t | a -> parent, a -> r where
  indexName :: Proxy a -> T.Text
  default indexName :: (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ]) => Proxy a -> T.Text
  indexName = gdConstrName

  createIndex ::
      (DynamoCollection a r t, DynamoIndex a parent r IsIndex, DynamoTable parent r2 t2, Code parent ~ '[ xs ': rest2 ],
        Code a ~ '[hash ': rest ])
         => Proxy a -> ProvisionedThroughput -> D.GlobalSecondaryIndex
  createIndex = defaultCreateIndex

palldynamo :: Proxy (All DynamoEncodable)
palldynamo = Proxy

pdynamo :: Proxy DynamoEncodable
pdynamo = Proxy

gdConstrName :: forall a xss. (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ])
  => Proxy a -> T.Text
gdConstrName _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs -> head $ hcollapse $ hliftA getName cs
    Newtype _ _ c -> head $ hcollapse $ hliftA getName (c :* Nil)
  where
    getName :: ConstructorInfo xs -> K T.Text xs
    getName (Record name _) = K (T.pack name)
    getName (Constructor name) = K (T.pack name)
    getName (Infix name _ _) = K (T.pack name)


-- | Function that translates haskell field names to database field names
translateFieldName :: String -> T.Text
translateFieldName = T.pack . translate
  where
    translate ('_':rest) = rest
    translate name
      | '_' `elem` name = drop 1 $ dropWhile (/= '_') name
      | otherwise = name

gdHashField :: forall a hash rest. (Generic a, HasDatatypeInfo a, Code a ~ '[ hash ': rest ] )
  => Proxy a -> (T.Text, Proxy hash)
gdHashField _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs -> head $ hcollapse $ hliftA getName cs
    Newtype _ _ c -> head $ hcollapse $ hliftA getName (c :* Nil)
  where
    getName :: ConstructorInfo xs -> K (T.Text, Proxy hash) xs
    getName (Record _ fields) =
        K $ (, Proxy) . head $ hcollapse $ hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

gdRangeField :: forall a hash range rest. (Generic a, HasDatatypeInfo a, Code a ~ '[ hash ': range ': rest ] )
  => Proxy a -> (T.Text, Proxy range)
gdRangeField _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs -> head $ hcollapse $ hliftA getName cs
    _ -> error "Cannot even patternmatch because of type error"
  where
    getName :: ConstructorInfo xs -> K (T.Text, Proxy range) xs
    getName (Record _ fields) =
        K $ (, Proxy) . (!! 1) $ hcollapse $ hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

gdFieldNames :: forall a xss rest. (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ': rest ]) => Proxy a -> NonEmpty T.Text
gdFieldNames _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs -> head $ hcollapse $ hliftA getName cs
    _ -> error "Cannot even patternmatch because of type error"
  where
    toNonEmpty :: [T.Text] -> NonEmpty T.Text
    toNonEmpty [] = error "Should not happen - checked at type level"
    toNonEmpty (x:xs) = x :| xs

    getName :: ConstructorInfo xs -> K (NonEmpty T.Text) xs
    getName (Record _ fields) =
        K $ toNonEmpty $ hcollapse $ hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

gdFieldNamesNP :: forall a xs. (HasDatatypeInfo a, Code a ~ '[ xs ]) => Proxy a -> NP (K T.Text) xs
gdFieldNamesNP _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs ->
        case hliftA getName cs of
          start :* Nil -> start
          _ -> error "Cannot happen - gdFieldNamesNP"
    _ -> error "Cannot even patternmatch because of type error"
  where
    getName :: ConstructorInfo xsd -> NP (K T.Text) xsd
    getName (Record _ fields) = hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

gdEncode :: forall a. (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a))
  => a -> [(T.Text, AttributeValue)]
gdEncode a =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs -> gdEncode' cs (from a)
    Newtype _ _ c -> gdEncode' (c :* Nil) (from a)
  where
    gdEncode' :: All2 DynamoEncodable xs => NP ConstructorInfo xs -> SOP I xs -> [(T.Text, AttributeValue)]
    gdEncode' cs (SOP sop) = hcollapse $ hcliftA2 palldynamo gdEncodeRec cs sop

    gdEncodeRec :: All DynamoEncodable xs => ConstructorInfo xs -> NP I xs -> K [(T.Text, AttributeValue)] xs
    gdEncodeRec (Record _ ns) xs =
        K $ hcollapse
          $ hcliftA2 pdynamo (\(FieldInfo name) (I val) -> K (T.pack name, dEncode val)) ns xs
    gdEncodeRec _ _ = error "Cannot serialize non-record types."

gdDecode ::
    forall a xs. (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a), Code a ~ '[ xs ])
  => HMap.HashMap T.Text AttributeValue -> Maybe a
gdDecode attrs =
    to . SOP . Z <$> hsequence (hcliftA dproxy decodeAttr (gdFieldNamesNP (Proxy :: Proxy a)))
  where
    decodeAttr :: DynamoEncodable b => K T.Text b -> Maybe b
    decodeAttr (K name) = dDecode (HMap.lookup name attrs)
    dproxy = Proxy :: Proxy DynamoEncodable

defaultPutItem ::
     (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a), DynamoTable a r t)
  => a -> D.PutItem
defaultPutItem item = D.putItem tblname & D.piItem .~ HMap.fromList attrs
  where
    attrs = gdEncode item
    tblname = tableName (pure item)

defaultCreateTable :: (Generic a, HasDatatypeInfo a, RecordOK (Code a) NoRange, Code a ~ '[ hash ': rest ])
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (gdConstrName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (firstname, firstproxy) = gdHashField p
    hashKey = keySchemaElement firstname D.Hash
    keyDefs = [D.attributeDefinition firstname (dType firstproxy)]

defaultCreateTableRange ::
    (Generic a, HasDatatypeInfo a, RecordOK (Code a) WithRange, Code a ~ '[ hash ': range ': rest ])
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTableRange p thr =
    D.createTable (gdConstrName p) (hashKey :| [rangeKey]) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (hashname, hashproxy) = gdHashField p
    (rangename, rangeproxy) = gdRangeField p
    hashKey = keySchemaElement hashname D.Hash
    rangeKey = keySchemaElement rangename D.Range
    keyDefs = [D.attributeDefinition hashname (dType hashproxy),
               D.attributeDefinition rangename (dType rangeproxy)]

defaultQueryKey :: (TableQuery a r t, Code a ~ '[ key ': rest ], DynamoCollection a r t, DynamoScalar key)
  => Proxy a -> key -> D.Query
defaultQueryKey p key =
  D.query (qTableName p) & D.qKeyConditionExpression .~ Just "#K = :key"
                         & D.qExpressionAttributeNames .~ HMap.singleton "K" hashname
                         & D.qExpressionAttributeValues .~ HMap.singleton "key" (dEncode key)
                         & D.qIndexName .~ qIndexName p
  where
    (hashname, _) = gdHashField p

defaultQueryKeyRange :: (TableQuery a r t, Code a ~ '[ hash ': range ': rest ], DynamoCollection a r t,
                          RecordOK (Code a) WithRange)
  => Proxy a -> (hash, RangeOper range) -> D.Query
defaultQueryKeyRange p (key, range) =
  D.query (qTableName p) & D.qKeyConditionExpression .~ Just condExpression
                         & D.qExpressionAttributeNames .~ attrnames
                         & D.qExpressionAttributeValues .~ attrvals
                         & D.qIndexName .~ qIndexName p
  where
    rangeSubst = "R"
    condExpression = "#K = :key AND <> " <> rangeOper range rangeSubst
    attrnames = HMap.fromList [("K", hashname), (rangeSubst, rangename)]
    attrvals = HMap.fromList $ rangeData range ++ [("key", dEncode key)]
    (hashname, _) = gdHashField p
    (rangename, _) = gdRangeField p

defaultCreateIndex :: forall a r parent r2 hash rest xs rest2 t t2.
  (DynamoCollection a r t, DynamoIndex a parent r IsIndex, DynamoTable parent r2 t2, Code parent ~ '[ xs ': rest2 ],
    Code a ~ '[hash ': rest ]) =>
  Proxy a -> ProvisionedThroughput -> D.GlobalSecondaryIndex
defaultCreateIndex p thr =
    globalSecondaryIndex (indexName p) keyschema proj thr
  where
    (hashname :| rest) = primaryFields (Proxy :: Proxy a)
    keyschema = keySchemaElement hashname D.Hash :| map (`keySchemaElement` D.Range) rest
    proj | Just lst <- nonEmpty attrlist =
                    D.projection & D.pProjectionType .~ Just D.Include
                                 & D.pNonKeyAttributes .~ Just lst
         | otherwise = D.projection & D.pProjectionType .~ Just D.KeysOnly
    parentKey = primaryFields (Proxy :: Proxy parent)
    attrlist = filter (`notElem` (toList parentKey ++ [hashname])) $ toList $ gdFieldNames (Proxy :: Proxy a)
