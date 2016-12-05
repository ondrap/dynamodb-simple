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
  , TableQuery
  , TableScan
  , TableCreate(..)
  , IndexCreate(..)
  , HasPrimaryKey(..)
  , createLocalIndex
) where

import           Control.Lens                     ((.~))
import           Data.Foldable                    (toList)
import           Data.Function                    ((&))
import qualified Data.HashMap.Strict              as HMap
import           Data.List.NonEmpty               (NonEmpty ((:|)), nonEmpty)
import           Data.Monoid                      ((<>))
import qualified Data.Text                        as T
import           Generics.SOP
import qualified Network.AWS.DynamoDB.CreateTable as D
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
class (Generic a, HasDatatypeInfo a, PrimaryFieldCount r, All2 DynamoEncodable (Code a))
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

class DynamoCollection a r t => HasPrimaryKey a (r :: RangeType) (t :: TableType) where
  dItemToKey :: (Code a ~ '[ hash ': range ': xss ]) => a -> PrimaryKey (Code a) r
  dKeyAndAttr :: (Code a ~ '[ hash ': range ': xss ])
            => Proxy a -> PrimaryKey (Code a) r -> HMap.HashMap T.Text D.AttributeValue

instance (DynamoCollection a 'NoRange t, Code a ~ '[ hash ': xss ],
          DynamoScalar v hash) => HasPrimaryKey a 'NoRange t where
  dItemToKey = gdFirstField
  dKeyAndAttr p key = HMap.singleton (fst $ gdHashField p) (dScalarEncode key)

instance (DynamoCollection a 'WithRange t, Code a ~ '[ hash ': range ': xss ],
          DynamoScalar v1 hash, DynamoScalar v2 range) => HasPrimaryKey a 'WithRange t where
  dItemToKey = gdTwoFields
  dKeyAndAttr p (key, range) = HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dScalarEncode key), (fst $ gdRangeField p, dScalarEncode range)]

-- | Descritpion of dynamo table
class DynamoCollection a r 'IsTable => DynamoTable a (r :: RangeType) | a -> r where
  -- | Dynamo table/index name; default is the constructor name
  tableName :: Proxy a -> T.Text
  default tableName :: (Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

  -- | Serialize data, put it into the database
  dPutItem :: a -> D.PutItem
  dPutItem = defaultPutItem

-- | Dispatch class for NoRange/WithRange createTable
class DynamoTable a r => TableCreate a (r :: RangeType) where
  createTable :: (Code a ~ '[ hash ': range ': xss ]) => Proxy a -> ProvisionedThroughput -> D.CreateTable
instance (DynamoTable a 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v hash)  => TableCreate a 'NoRange where
  createTable = defaultCreateTable
instance (DynamoTable a 'WithRange, Code a ~ '[ hash ': range ': xss ], DynamoScalar v1 hash, DynamoScalar v2 range)
    => TableCreate a 'WithRange where
  createTable = defaultCreateTableRange

-- | Instance for tables that can be queried
class DynamoCollection a 'WithRange t => TableQuery a (t :: TableType) where
  -- | Return table name and index name
  qTableName :: Proxy a -> T.Text
  qIndexName :: Proxy a -> Maybe T.Text
  -- | Create a query using both hash key and operation on range key
  -- On tables without range key, this degrades to queryKey
  dQueryKey :: (Code a ~ '[ hash ': range ': rest ])
      => Proxy a -> hash -> Maybe (RangeOper range) -> D.Query
instance  (DynamoCollection a 'WithRange 'IsTable, DynamoTable a 'WithRange,
           Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
    => TableQuery a 'IsTable where
  dQueryKey = defaultQueryKey
  qTableName = tableName
  qIndexName _ = Nothing
instance  (DynamoCollection a 'WithRange 'IsIndex, DynamoIndex a parent 'WithRange, DynamoTable parent r1,
            Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
        => TableQuery a 'IsIndex where
  dQueryKey = defaultQueryKey
  qTableName _ = tableName (Proxy :: Proxy parent)
  qIndexName = Just . indexName

class DynamoCollection a r t => TableScan a (r :: RangeType) (t :: TableType) where
  -- | Return table name and index name
  qsTableName :: Proxy a -> T.Text
  qsIndexName :: Proxy a -> Maybe T.Text
  dScan :: Proxy a -> D.Scan
instance DynamoTable a r => TableScan a r 'IsTable where
  qsTableName = tableName
  qsIndexName _ = Nothing
  dScan = defaultScan
instance (DynamoCollection a r 'IsIndex,
          DynamoIndex a parent r, DynamoTable parent r1) => TableScan a r 'IsIndex where
  qsTableName _ = tableName (Proxy :: Proxy parent)
  qsIndexName = Just . indexName
  dScan = defaultScan

-- | Parameter type for queryKeyRange
type family PrimaryKey (a :: [[*]]) (r :: RangeType) :: * where
    PrimaryKey ('[ key ': range ': rest ] ) 'WithRange = (key, range)
    PrimaryKey ('[ key ': rest ]) 'NoRange = key

-- | Class representing a Global Secondary Index
class DynamoCollection a r 'IsIndex => DynamoIndex a parent (r :: RangeType) | a -> parent r where
  indexName :: Proxy a -> T.Text
  default indexName :: (Code a ~ '[ xss ]) => Proxy a -> T.Text
  indexName = gdConstrName

class DynamoIndex a parent r => IndexCreate a parent (r :: RangeType) where
  createGlobalIndex ::
    (DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ], Code a ~ '[hash ': range ': rest ])
             => Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])

instance (DynamoIndex a p 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v hash) => IndexCreate a p 'NoRange where
  createGlobalIndex = defaultCreateGlobalIndex
instance (DynamoIndex a p 'WithRange, Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
      => IndexCreate a p 'WithRange where
  createGlobalIndex = defaultCreateGlobalIndexRange

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
  where
    getName :: ConstructorInfo xs -> K (T.Text, Proxy range) xs
    getName (Record _ fields) =
        K $ (, Proxy) . (!! 1) $ hcollapse $ hliftA (\(FieldInfo name) -> K (translateFieldName name)) fields
    getName _ = error "Only records are supported."

-- | Return first field of a datatype
gdFirstField :: forall a hash rest. (Generic a, Code a ~ '[ hash ': rest ]) => a -> hash
gdFirstField item = firstField (from item)
  where
    firstField :: (xs ~ '[ hash ': rest ]) => SOP I xs -> hash
    firstField (SOP (Z (start :* _))) = unI start
    firstField (SOP (S _)) = error "This cannot happen." -- or the signature is not enough?

-- | Return first 2 fields of a datatype
gdTwoFields :: forall a hash range rest. (Generic a, Code a ~ '[ hash ': range ': rest ])
    => a -> (hash, range)
gdTwoFields item = twoFields (from item)
  where
    twoFields :: (xs ~ '[ hash ': range ': rest ]) => SOP I xs -> (hash, range)
    twoFields (SOP (Z (start :* range :* _))) = (unI start, unI range)
    twoFields (SOP (S _)) = error "This cannot happen." -- or the signature is not good enough?


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

defaultCreateTable :: (DynamoTable a 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v hash)
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (gdConstrName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (firstname, firstproxy) = gdHashField p
    hashKey = keySchemaElement firstname D.Hash
    keyDefs = [D.attributeDefinition firstname (dType firstproxy)]

defaultCreateTableRange :: (DynamoTable a 'WithRange, Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
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

defaultQueryKey :: (TableQuery a t, Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
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

defaultCreateGlobalIndex :: forall a r parent r2 hash rest xs rest2 v.
  (DynamoIndex a parent r, DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ],
    Code a ~ '[hash ': rest ], DynamoScalar v hash) =>
  Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
defaultCreateGlobalIndex p thr =
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

mkIndexHelper :: forall a parent r2 hash rest xs rest2 range v1 v2.
  (DynamoIndex a parent 'WithRange, DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ],
    Code a ~ '[hash ': range ': rest ],
    DynamoScalar v1 hash, DynamoScalar v2 range) =>
  Proxy a -> (NonEmpty D.KeySchemaElement, D.Projection, [D.AttributeDefinition])
mkIndexHelper p = (keyschema, proj, attrdefs)
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
    attrlist = filter (`notElem` (toList parentKey ++ [hashname, rangename])) $ toList $ gdFieldNames (Proxy :: Proxy a)

defaultCreateGlobalIndexRange :: forall a parent r2 hash rest xs rest2 range v1 v2.
  (DynamoIndex a parent 'WithRange, DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ],
    Code a ~ '[hash ': range ': rest ],
    DynamoScalar v1 hash, DynamoScalar v2 range) =>
  Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
defaultCreateGlobalIndexRange p thr =
    (globalSecondaryIndex (indexName p) keyschema proj thr, attrdefs)
  where
    (keyschema, proj, attrdefs) = mkIndexHelper p

createLocalIndex :: forall a parent r2 hash rest xs rest2 range v1 v2.
  (DynamoIndex a parent 'WithRange, DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ],
    Code a ~ '[hash ': range ': rest ],
    DynamoScalar v1 hash, DynamoScalar v2 range) =>
  Proxy a -> (D.LocalSecondaryIndex, [D.AttributeDefinition])
createLocalIndex p =
    (D.localSecondaryIndex (indexName p) keyschema proj, attrdefs)
  where
    (keyschema, proj, attrdefs) = mkIndexHelper p


defaultScan :: (TableScan a r t) => Proxy a -> D.Scan
defaultScan p = D.scan (qsTableName p) & D.sIndexName .~ qsIndexName p
