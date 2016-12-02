{-# LANGUAGE CPP                    #-}
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE PolyKinds              #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TupleSections          #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
#if __GLASGOW_HASKELL__ >= 800
{-# LANGUAGE UndecidableSuperClasses #-}
#endif

module Database.DynamoDB.Class (
    DynamoCollection(..)
  , DynamoTable(..)
  , DynamoIndex(..)
  , dQueryKey
  , dScan
  , RangeType(..)
  , TableType(..)
  , gdDecode
  , translateFieldName
  , PrimaryKey
  , RecordOK
  , ItemOper(..)
  , TableQuery
  , TableScan
  , TableCreate(..)
) where

import           Control.Lens                     ((.~))
import           Data.Foldable                    (toList)
import           Data.Function                    ((&))
import qualified Data.HashMap.Strict              as HMap
import           Data.List.NonEmpty               (NonEmpty ((:|)), nonEmpty)
import           Data.Monoid                      ((<>))
import qualified Data.Text                        as T
import           Generics.SOP
import           GHC.Exts                         (Constraint)
import qualified Network.AWS.DynamoDB.CreateTable as D
import qualified Network.AWS.DynamoDB.DeleteTable as D
import qualified Network.AWS.DynamoDB.PutItem     as D
import qualified Network.AWS.DynamoDB.Query       as D
import qualified Network.AWS.DynamoDB.Scan        as D
import           Network.AWS.DynamoDB.Types       (ProvisionedThroughput,
                                                   globalSecondaryIndex,
                                                   keySchemaElement)
import qualified Network.AWS.DynamoDB.Types       as D


import           Database.DynamoDB.Internal       (rangeOper, rangeData)
import           Database.DynamoDB.Types


-- | Data collection type - with hash key or with hash+sort key
data RangeType = NoRange | WithRange

-- | Helper type to distinguish index and table collections
data TableType = IsTable | IsIndex

-- | Basic instance for dynamo collection (table or index)
-- This instances fixes the tableName and the sort key
class (Generic a, HasDatatypeInfo a, PrimaryFieldCount r, All2 DynamoEncodable (Code a),
       RecordOK (Code a) r)
  => DynamoCollection a (r :: RangeType) (t :: TableType) | a -> r t where
  primaryFields :: (Code a ~ '[ xs ': rest ]) => Proxy a -> NonEmpty T.Text
  primaryFields _ = key :| take (primaryFieldCount (Proxy :: Proxy r) - 1) rest
    where
      (key :| rest) = gdFieldNames (Proxy :: Proxy a)

class PrimaryFieldCount (r :: RangeType) where
  primaryFieldCount :: Proxy r -> Int
instance PrimaryFieldCount 'NoRange where
  primaryFieldCount _ = 1
instance PrimaryFieldCount 'WithRange where
  primaryFieldCount _ = 2

-- | Descritpion of dynamo table
class DynamoCollection a r 'IsTable => DynamoTable a (r :: RangeType) | a -> r where
  -- | Dynamo table/index name; default is the constructor name
  tableName :: Proxy a -> T.Text
  default tableName :: (Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

  -- | Serialize data, put it into the database
  dPutItem :: a -> D.PutItem
  dPutItem = defaultPutItem

  deleteTable :: Proxy a -> D.DeleteTable
  deleteTable p = D.deleteTable (tableName p)


-- | Dispatch class for NoRange/WithRange deleteItem
class DynamoTable a r => ItemOper a (r :: RangeType) where
  dKeyAndAttr :: (Code a ~ '[ hash ': range ': xss ])
            => Proxy a -> PrimaryKey (Code a) r -> HMap.HashMap T.Text D.AttributeValue

instance DynamoTable a 'NoRange => ItemOper a 'NoRange where
  dKeyAndAttr p key = HMap.singleton (fst $ gdHashField p) (dScalarEncode key)

instance DynamoTable a 'WithRange => ItemOper a 'WithRange where
  dKeyAndAttr p (key, range) = HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dScalarEncode key), (fst $ gdRangeField p, dScalarEncode range)]

-- | Dispatch class for NoRange/WithRange createTable
class DynamoTable a r => TableCreate a (r :: RangeType) where
  createTable :: (Code a ~ '[ hash ': range ': xss ]) => Proxy a -> ProvisionedThroughput -> D.CreateTable
instance DynamoTable a 'NoRange => TableCreate a 'NoRange where
  createTable = defaultCreateTable
instance DynamoTable a 'WithRange => TableCreate a 'WithRange where
  createTable = defaultCreateTableRange

-- | Instance for tables that can be queried
class DynamoCollection a 'WithRange t => TableQuery a (t :: TableType) where
  -- | Return table name and index name
  qTableName :: Proxy a -> T.Text
  qIndexName :: Proxy a -> Maybe T.Text
  -- | Create a query using both hash key and operation on range key
  -- On tables without range key, this degrades to queryKey
  dQueryKey :: (TableQuery a t, Code a ~ '[ hash ': range ': rest ])
      => Proxy a -> hash -> Maybe (RangeOper range) -> D.Query
instance  (DynamoTable a 'WithRange, DynamoCollection a 'WithRange 'IsTable,
           Code a ~ '[ xs ]) => TableQuery a 'IsTable where
  dQueryKey = defaultQueryKey
  qTableName = tableName
  qIndexName _ = Nothing
instance  (RecordOK (Code a) 'WithRange, DynamoIndex a parent 'WithRange 'IsIndex,
            DynamoTable parent r1,
            DynamoCollection a 'WithRange 'IsIndex, Code a ~ '[ xs ]) => TableQuery a 'IsIndex where
  dQueryKey = defaultQueryKey
  qTableName _ = tableName (Proxy :: Proxy parent)
  qIndexName = Just . indexName

class (DynamoCollection a r t, All2 DynamoEncodable (Code a))
      => TableScan a (r :: RangeType) (t :: TableType) where
  -- | Return table name and index name
  qsTableName :: Proxy a -> T.Text
  qsIndexName :: Proxy a -> Maybe T.Text
  dScan :: Proxy a -> D.Scan
instance (DynamoCollection a r 'IsTable, DynamoTable a r) => TableScan a r 'IsTable where
  qsTableName = tableName
  qsIndexName _ = Nothing
  dScan = defaultScan
instance (DynamoCollection a r 'IsIndex, DynamoIndex a parent r 'IsIndex,
          DynamoTable parent r1, All2 DynamoEncodable (Code a))
          => TableScan a r 'IsIndex where
  qsTableName _ = tableName (Proxy :: Proxy parent)
  qsIndexName = Just . indexName
  dScan = defaultScan

-- | Parameter type for queryKeyRange
type family PrimaryKey (a :: [[*]]) (r :: RangeType) :: * where
    PrimaryKey ('[ key ': range ': rest ] ) 'WithRange = (key, range)
    PrimaryKey ('[ key ': rest ]) 'NoRange = key

-- | Constraint to check that hash/sort key are scalar
type family RecordOK (a :: [[*]]) (r :: RangeType) :: Constraint where
    RecordOK '[ hash ': rest ] 'NoRange = (DynamoScalar hash, All DynamoEncodable rest)
    RecordOK '[ hash ': range ': rest ] 'WithRange = (DynamoScalar hash, DynamoScalar range, All DynamoEncodable rest)

-- | Class representing a Global Secondary Index
class DynamoCollection a r t
        => DynamoIndex a parent (r :: RangeType) (t :: TableType) | a -> parent r t where
  indexName :: Proxy a -> T.Text
  default indexName :: (Code a ~ '[ xss ]) => Proxy a -> T.Text
  indexName = gdConstrName

  createIndex ::
      (DynamoCollection a r t, DynamoIndex a parent r 'IsIndex, DynamoTable parent r2,
        Code parent ~ '[ xs ': rest2 ],
        RecordOK (Code a) r, Code a ~ '[hash ': range ': rest ], IndexCreate a r)
         => Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
  createIndex = iCreateIndex (Proxy :: Proxy r)

class IndexCreate a (r :: RangeType) where
  iCreateIndex ::
    (DynamoCollection a r t, DynamoIndex a parent r 'IsIndex, DynamoTable parent r2,
      Code parent ~ '[ xs ': rest2 ], RecordOK (Code a) r, Code a ~ '[hash ': range ': rest ])
             => Proxy r -> Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
instance IndexCreate a 'NoRange where
  iCreateIndex _ = defaultCreateIndex
instance IndexCreate a 'WithRange where
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

defaultPutItem :: forall a r. DynamoTable a r => a -> D.PutItem
defaultPutItem item = D.putItem tblname & D.piItem .~ gdEncode item
  where
    tblname = tableName (Proxy :: Proxy a)

defaultCreateTable :: (DynamoTable a 'NoRange, Code a ~ '[ hash ': rest ])
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (gdConstrName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (firstname, firstproxy) = gdHashField p
    hashKey = keySchemaElement firstname D.Hash
    keyDefs = [D.attributeDefinition firstname (dType firstproxy)]

defaultCreateTableRange :: (DynamoTable a 'WithRange, Code a ~ '[ hash ': range ': rest ])
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

defaultQueryKey :: (TableQuery a t, Code a ~ '[ hash ': range ': rest ],
                    RecordOK (Code a) 'WithRange)
  => Proxy a -> hash -> Maybe (RangeOper range) -> D.Query
defaultQueryKey p key Nothing =
  D.query (qTableName p) & D.qKeyConditionExpression .~ Just "#K = :key"
                         & D.qExpressionAttributeNames .~ HMap.singleton "#K" hashname
                         & D.qExpressionAttributeValues .~ HMap.singleton ":key" (dScalarEncode key)
                         & D.qIndexName .~ qIndexName p
  where
    (hashname, _) = gdHashField p
defaultQueryKey p key (Just range) =
  D.query (qTableName p) & D.qKeyConditionExpression .~ Just condExpression
                         & D.qExpressionAttributeNames .~ attrnames
                         & D.qExpressionAttributeValues .~ attrvals
                         & D.qIndexName .~ qIndexName p
  where
    rangeSubst = "#R"
    condExpression = "#K = :key AND <> " <> rangeOper range rangeSubst
    attrnames = HMap.fromList [("#K", hashname), (rangeSubst, rangename)]
    attrvals = HMap.fromList $ rangeData range ++ [(":key", dScalarEncode key)]
    (hashname, _) = gdHashField p
    (rangename, _) = gdRangeField p

defaultCreateIndex :: forall a r parent r2 hash rest xs rest2.
  (DynamoIndex a parent r 'IsIndex, DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ],
    RecordOK (Code a) 'NoRange, Code a ~ '[hash ': rest ]) =>
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

defaultCreateIndexRange :: forall a r parent r2 hash rest xs rest2 range.
  (DynamoIndex a parent r 'IsIndex, DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ],
    RecordOK (Code a) 'WithRange, Code a ~ '[hash ': range ': rest ]) =>
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

defaultScan :: (TableScan a r t) => Proxy a -> D.Scan
defaultScan p = D.scan (qsTableName p) & D.sIndexName .~ qsIndexName p
