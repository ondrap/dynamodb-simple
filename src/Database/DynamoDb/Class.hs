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
  , dQueryKey, dQueryKeyRange
  , NoRange, WithRange
  , IsTable, IsIndex
  , gdDecode
  , translateFieldName
  , PrimaryKey
  , RecordOK
  , ItemOper
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
import qualified Network.AWS.DynamoDB.GetItem  as D
import qualified Network.AWS.DynamoDB.PutItem     as D
import           Network.AWS.DynamoDB.Types       (ProvisionedThroughput, keySchemaElement,
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
class (TableCreate a r, DynamoCollection a r t, Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a),
       RecordOK (Code a) r, ItemOper a r) => DynamoTable a r t | a -> r where
  -- | Dynamo table/index name; default is the constructor name
  tableName :: Proxy a -> T.Text
  default tableName :: (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

  -- | Serialize data, put it into the database
  dPutItem :: a -> D.PutItem
  dPutItem = defaultPutItem

  -- |
  createTable :: Code a ~ '[ hash ': range ': rest ] => Proxy a -> ProvisionedThroughput -> D.CreateTable
  createTable = iCreateTable (Proxy :: Proxy r)

  deleteTable :: Proxy a -> D.DeleteTable
  deleteTable p = D.deleteTable (tableName p)

  dDeleteItem :: (Code a ~ '[ key ': hash ': rest ]) => Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
  dDeleteItem = iDeleteItem (Proxy :: Proxy r)

  dDeleteRequest :: (Code a ~ '[ key ': hash ': rest ]) => Proxy a -> PrimaryKey (Code a) r -> D.DeleteRequest
  dDeleteRequest = iDeleteRequest (Proxy :: Proxy r)

  dGetItem :: (Code a ~ '[ key ': hash ': rest ]) => Proxy a -> PrimaryKey (Code a) r -> D.GetItem
  dGetItem = iGetItem (Proxy :: Proxy r)

-- | Dispatch class for NoRange/WithRange deleteItem
class ItemOper a r where
  iDeleteItem :: (DynamoTable a r t, Code a ~ '[ hash ': range ': xss ])
            => Proxy r -> Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
  iDeleteRequest :: (DynamoTable a r t, Code a ~ '[ hash ': range ': xss ])
            => Proxy r -> Proxy a -> PrimaryKey (Code a) r -> D.DeleteRequest
  iGetItem :: (DynamoTable a r t, Code a ~ '[ hash ': range ': xss ])
            => Proxy r -> Proxy a -> PrimaryKey (Code a) r -> D.GetItem

instance ItemOper a NoRange where
  iDeleteItem _ p key =
      D.deleteItem (tableName p) & D.diKey .~ HMap.singleton (fst $ gdHashField p) (dScalarEncode key)
  iDeleteRequest _ p key =
      D.deleteRequest & D.drKey .~ HMap.singleton (fst $ gdHashField p) (dScalarEncode key)
  iGetItem _ p key =
      D.getItem (tableName p) & D.giKey .~ HMap.singleton (fst $ gdHashField p) (dScalarEncode key)
instance ItemOper a WithRange where
  iDeleteItem _ p (key, range) = D.deleteItem (tableName p) & D.diKey .~ HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dScalarEncode key), (fst $ gdRangeField p, dScalarEncode range)]
  iDeleteRequest _ p (key, range) = D.deleteRequest & D.drKey .~ HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dScalarEncode key), (fst $ gdRangeField p, dScalarEncode range)]
  iGetItem _ p (key, range) = D.getItem (tableName p) & D.giKey .~ HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dScalarEncode key), (fst $ gdRangeField p, dScalarEncode range)]


-- | Dispatch class for NoRange/WithRange createTable
class TableCreate a r where
  iCreateTable :: (DynamoTable a r t, Code a ~ '[ hash ': range ': xss ])
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
  dQueryKey :: (Code a ~ '[ key ': rest ], DynamoScalar key) => Proxy a -> key -> D.Query
  dQueryKey = defaultQueryKey
  -- | Create a query using both hash key and operation on range key
  -- On tables without range key, this degrades to queryKey
  dQueryKeyRange ::
        (TableQuery a r t, Code a ~ '[ key ': hash ': rest ], RecordOK (Code a) r)
      => Proxy a -> QueryRange (Code a) r -> D.Query

instance (DynamoTable a NoRange IsTable, Code a ~ '[ xs ]) => TableQuery a NoRange IsTable where
  type QueryRange ('[ key ': rest ]) NoRange  = key
  dQueryKeyRange = defaultQueryKey
  qTableName = tableName
  qIndexName _ = Nothing
instance  (DynamoTable a WithRange IsTable, Code a ~ '[ xs ]) => TableQuery a WithRange IsTable where
  type QueryRange ('[ key ': range ': rest ] ) WithRange  = (key, RangeOper range)
  dQueryKeyRange = defaultQueryKeyRange
  qTableName = tableName
  qIndexName _ = Nothing
instance (DynamoIndex a parent NoRange IsIndex, DynamoTable parent r1 t1, DynamoCollection a NoRange IsIndex, Code a ~ '[ xs ]) => TableQuery a NoRange IsIndex where
  type QueryRange ('[ key ': rest ]) NoRange  = key
  dQueryKeyRange = defaultQueryKey
  qTableName _ = tableName (Proxy :: Proxy parent)
  qIndexName = Just . indexName
instance  (DynamoIndex a parent WithRange IsIndex, DynamoTable parent r1 t1, DynamoCollection a WithRange IsIndex, Code a ~ '[ xs ]) => TableQuery a WithRange IsIndex where
  type QueryRange ('[ key ': range ': rest ] ) WithRange  = (key, RangeOper range)
  dQueryKeyRange = defaultQueryKeyRange
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
        RecordOK (Code a) r, Code a ~ '[hash ': range ': rest ], IndexCreate a r)
         => Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
  createIndex = iCreateIndex (Proxy :: Proxy r)

class IndexCreate a r where
  iCreateIndex ::
    (DynamoCollection a r t, DynamoIndex a parent r IsIndex, DynamoTable parent r2 t2, Code parent ~ '[ xs ': rest2 ],
      RecordOK (Code a) r, Code a ~ '[hash ': range ': rest ])
             => Proxy r -> Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
instance IndexCreate a NoRange where
  iCreateIndex _ = defaultCreateIndex
instance IndexCreate a WithRange where
  iCreateIndex _ = defaultCreateIndexRange


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

defaultPutItem :: forall a r t. DynamoTable a r t => a -> D.PutItem
defaultPutItem item = D.putItem tblname & D.piItem .~ gdEncode item
  where
    tblname = tableName (Proxy :: Proxy a)

defaultCreateTable :: (DynamoTable a NoRange t, Code a ~ '[ hash ': rest ])
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (gdConstrName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (firstname, firstproxy) = gdHashField p
    hashKey = keySchemaElement firstname D.Hash
    keyDefs = [D.attributeDefinition firstname (dType firstproxy)]

defaultCreateTableRange :: (DynamoTable a WithRange t, Code a ~ '[ hash ': range ': rest ])
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
                         & D.qExpressionAttributeValues .~ HMap.singleton "key" (dScalarEncode key)
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
    attrvals = HMap.fromList $ rangeData range ++ [("key", dScalarEncode key)]
    (hashname, _) = gdHashField p
    (rangename, _) = gdRangeField p

defaultCreateIndex :: forall a r parent r2 hash rest xs rest2 t t2.
  (DynamoCollection a r t, DynamoIndex a parent r IsIndex, DynamoTable parent r2 t2, Code parent ~ '[ xs ': rest2 ],
    RecordOK (Code a) NoRange, Code a ~ '[hash ': rest ]) =>
  Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
defaultCreateIndex p thr =
    (globalSecondaryIndex (indexName p) keyschema proj thr, attrdefs)
  where
    (hashname, hashproxy) = gdHashField p
    attrdefs = [D.attributeDefinition hashname (dType hashproxy)]
    keyschema = keySchemaElement hashname D.Hash :| []
    proj | Just lst <- nonEmpty attrlist =
                    D.projection & D.pProjectionType .~ Just D.Include
                                 & D.pNonKeyAttributes .~ Just lst
         | otherwise = D.projection & D.pProjectionType .~ Just D.KeysOnly
    parentKey = primaryFields (Proxy :: Proxy parent)
    attrlist = filter (`notElem` (toList parentKey ++ [hashname])) $ toList $ gdFieldNames (Proxy :: Proxy a)

defaultCreateIndexRange :: forall a r parent r2 hash rest xs rest2 t t2 range.
  (DynamoCollection a r t, DynamoIndex a parent r IsIndex, DynamoTable parent r2 t2, Code parent ~ '[ xs ': rest2 ],
    RecordOK (Code a) WithRange, Code a ~ '[hash ': range ': rest ]) =>
  Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
defaultCreateIndexRange p thr =
    (globalSecondaryIndex (indexName p) keyschema proj thr, attrdefs)
  where
    (hashname, hashproxy) = gdHashField p
    (rangename, rangeproxy) = gdRangeField p
    attrdefs = [D.attributeDefinition hashname (dType hashproxy), D.attributeDefinition rangename (dType rangeproxy)]
    --
    keyschema = keySchemaElement hashname D.Hash :| [keySchemaElement rangename D.Range]
    --
    proj | Just lst <- nonEmpty attrlist =
                    D.projection & D.pProjectionType .~ Just D.Include
                                 & D.pNonKeyAttributes .~ Just lst
         | otherwise = D.projection & D.pProjectionType .~ Just D.KeysOnly
    parentKey = primaryFields (Proxy :: Proxy parent)
    attrlist = filter (`notElem` (toList parentKey ++ [hashname])) $ toList $ gdFieldNames (Proxy :: Proxy a)
