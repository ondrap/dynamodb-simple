{-# LANGUAGE CPP                    #-}
#if __GLASGOW_HASKELL__ >= 800
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
#endif
{-# OPTIONS_GHC -fno-warn-incomplete-uni-patterns #-}
-- We have lots of pattern matching for allFieldNames, that is correct because of TH
{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE GADTs                  #-}
{-# LANGUAGE MultiParamTypeClasses  #-}
{-# LANGUAGE OverloadedStrings      #-}
{-# LANGUAGE PolyKinds              #-}
{-# LANGUAGE RankNTypes             #-}
{-# LANGUAGE ScopedTypeVariables    #-}
{-# LANGUAGE TypeFamilies           #-}
{-# LANGUAGE TypeOperators          #-}
{-# LANGUAGE UndecidableInstances   #-}
{-# LANGUAGE DefaultSignatures   #-}
#if __GLASGOW_HASKELL__ >= 800
{-# LANGUAGE UndecidableSuperClasses #-}
#endif

module Database.DynamoDB.Class (
    DynamoCollection(..)
  , DynamoTable(..)
  , DynamoIndex(..)
  , defaultScan
  , RangeType(..)
  , TableType(..)
  , gsDecode
  , gsDecodeG
  , gsEncode
  , gsEncodeG
  , PrimaryKey
  , CanQuery(..)
  , TableScan
  , TableCreate(..)
  , IndexCreate(..)
  , HasPrimaryKey(..)
  , createLocalIndex
  , ContainsTableKey(..)
) where

import           Control.Lens                     ((.~), sequenceOf, _2)
import           Data.Bifunctor                   (first)
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
                                                   keySchemaElement, AttributeValue)
import qualified Network.AWS.DynamoDB.Types       as D
import Data.HashMap.Strict (HashMap)
import Data.Maybe (mapMaybe)


import           Database.DynamoDB.Internal       (rangeOper, rangeData)
import           Database.DynamoDB.Types


-- | Data collection type - with hash key or with hash+sort key
data RangeType = NoRange | WithRange

-- | Helper type to distinguish index and table collections
data TableType = IsTable | IsIndex

-- | Basic instance for dynamo collection (table or index)
-- This instances fixes the tableName and the sort key
class (Generic a, All2 DynamoEncodable (Code a))
  => DynamoCollection a (r :: RangeType) (t :: TableType) | a -> r t where
  allFieldNames :: Proxy a -> [T.Text]
  primaryFields :: Proxy a -> [T.Text]
  dGsDecode :: HashMap T.Text AttributeValue -> Either T.Text a
  default dGsDecode :: (Code a ~ '[ xs ]) => HashMap T.Text AttributeValue -> Either T.Text a
  dGsDecode = gsDecode

class DynamoCollection a r t => HasPrimaryKey a (r :: RangeType) (t :: TableType) where
  dItemToKey :: a -> PrimaryKey a r
  dKeyToAttr :: Proxy a -> PrimaryKey a r -> HMap.HashMap T.Text D.AttributeValue
  dAttrToKey :: Proxy a -> HMap.HashMap T.Text D.AttributeValue -> Maybe (PrimaryKey a r)
  -- | Return True if key is well-defined (i.e. no empty string, no Maybe etc.)
  dKeyIsDefined :: Proxy a -> PrimaryKey a r -> Bool

instance (DynamoCollection a 'NoRange t, Code a ~ '[ hash ': xss ],
          DynamoScalar v hash) => HasPrimaryKey a 'NoRange t where
  dItemToKey = gdFirstField
  dKeyToAttr p key = HMap.singleton (head $ allFieldNames p) (dScalarEncode key)
  dAttrToKey p attrs = HMap.lookup (head $ allFieldNames p) attrs >>= dDecode . Just
  dKeyIsDefined _ = not . dIsMissing

instance (DynamoCollection a 'WithRange t, Code a ~ '[ hash ': range ': xss ],
          DynamoScalar v1 hash, DynamoScalar v2 range) => HasPrimaryKey a 'WithRange t where
  dItemToKey = gdTwoFields
  dKeyToAttr p (key, range) = HMap.fromList plist
    where
      (hashname:rangename:_) = allFieldNames p
      plist = [(hashname, dScalarEncode key), (rangename, dScalarEncode range)]
  dAttrToKey p attrs = do
      let (hashname:rangename:_) = allFieldNames p
      pkey <- HMap.lookup hashname attrs
      pval <- dDecode (Just pkey)
      rngkey <- HMap.lookup rangename attrs
      rngval <- dDecode (Just rngkey)
      return (pval, rngval)
  dKeyIsDefined _ (hash, range) = not (dIsMissing hash) && not (dIsMissing range)

-- | Descritpion of dynamo table
class (DynamoCollection a r 'IsTable, HasPrimaryKey a r 'IsTable) => DynamoTable a (r :: RangeType) | a -> r where
  -- | Dynamo table/index name; default is the constructor name
  tableName :: Proxy a -> T.Text
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

-- | Class for tables/indexes that can be queried
class (DynamoCollection a 'WithRange t, HasPrimaryKey a 'WithRange t) => CanQuery a (t :: TableType) hash range | a -> hash range where
  -- | Return table name and index name
  qTableName :: Proxy a -> T.Text
  qIndexName :: Proxy a -> Maybe T.Text
  -- | Create a query using both hash key and operation on range key
  -- On tables without range key, this degrades to queryKey
  dQueryKey :: Proxy a -> hash -> Maybe (RangeOper range) -> D.Query
  dQueryKeyToAttr :: Proxy a -> (hash, range) -> HMap.HashMap T.Text D.AttributeValue
instance  (DynamoCollection a 'WithRange 'IsTable, DynamoTable a 'WithRange,
           Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range,
           PrimaryKey a 'WithRange ~ (hash, range))
    => CanQuery a 'IsTable hash range where
  dQueryKey = defaultQueryKey
  qTableName = tableName
  qIndexName _ = Nothing
  dQueryKeyToAttr = dKeyToAttr
instance  (DynamoCollection a 'WithRange 'IsIndex, DynamoIndex a parent 'WithRange, DynamoTable parent r1,
            Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range,
            PrimaryKey a 'WithRange ~ (hash, range))
        => CanQuery a 'IsIndex hash range where
  dQueryKey = defaultQueryKey
  qTableName _ = tableName (Proxy :: Proxy parent)
  qIndexName = Just . indexName
  dQueryKeyToAttr = dKeyToAttr

-- | Class for tables/indexes that can be scanned
class (DynamoCollection a r t, HasPrimaryKey a r t) => TableScan a (r :: RangeType) (t :: TableType) where
  -- | Return table name and index name
  qsTableName :: Proxy a -> T.Text
  qsIndexName :: Proxy a -> Maybe T.Text
instance (DynamoCollection a r 'IsTable, DynamoTable a r) => TableScan a r 'IsTable where
  qsTableName = tableName
  qsIndexName _ = Nothing
instance (DynamoCollection a r 'IsIndex,
          DynamoIndex a parent r, DynamoTable parent r1, HasPrimaryKey a r 'IsIndex) => TableScan a r 'IsIndex where
  qsTableName _ = tableName (Proxy :: Proxy parent)
  qsIndexName = Just . indexName

-- | Type family that returns a primary key of a table/index depending on the 'RangeType' parameter.
type PrimaryKey a r = PrimaryKey' (Code a) r
-- | Parameter type for queryKeyRange
type family PrimaryKey' (a :: [[*]]) (r :: RangeType) :: * where
    PrimaryKey' ('[ key ': range ': rest ] ) 'WithRange = (key, range)
    PrimaryKey' ('[ key ': rest ]) 'NoRange = key

-- | Class representing a Global Secondary Index
class DynamoCollection a r 'IsIndex => DynamoIndex a parent (r :: RangeType) | a -> parent r where
  indexName :: Proxy a -> T.Text
  indexIsLocal :: Proxy a -> Bool

class DynamoIndex a parent r => IndexCreate a parent (r :: RangeType) where
  createGlobalIndex ::
    (DynamoTable parent r2, Code parent ~ '[ xs ': rest2 ], Code a ~ '[hash ': range ': rest ])
             => Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])

instance (DynamoIndex a p 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v hash) => IndexCreate a p 'NoRange where
  createGlobalIndex = defaultCreateGlobalIndex
instance (DynamoIndex a p 'WithRange, Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
      => IndexCreate a p 'WithRange where
  createGlobalIndex = defaultCreateGlobalIndexRange

-- | Class for indexes that contain primary key of the parent table
class ContainsTableKey a parent key | a -> parent key where
  -- | Extract table primary key from an item, if possible
  dTableKey :: a -> key

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

gsEncode :: forall a r t. DynamoCollection a r t => a -> HashMap T.Text AttributeValue
gsEncode = gsEncodeG (allFieldNames (Proxy :: Proxy a))

gsEncodeG :: forall a. (Generic a, All2 DynamoEncodable (Code a))
  => [T.Text] -> a -> HashMap T.Text AttributeValue
gsEncodeG names a = HMap.fromList $ mapMaybe (sequenceOf _2) $ zip names (gsEncode' (from a))
  where
    gsEncode' :: All2 DynamoEncodable xs => SOP I xs -> [Maybe AttributeValue]
    gsEncode' (SOP sop) = hcollapse $ hcliftA palldynamo gsEncodeRec sop

    gsEncodeRec :: All DynamoEncodable xss => NP I xss -> K [Maybe AttributeValue] xss
    gsEncodeRec = K . hcollapse . hcliftA pdynamo (K . dEncode . unI)

    palldynamo :: Proxy (All DynamoEncodable)
    palldynamo = Proxy

    pdynamo :: Proxy DynamoEncodable
    pdynamo = Proxy


gsDecode :: forall a r t xs. (DynamoCollection a r t, Code a ~ '[ xs ])
  => HashMap T.Text AttributeValue -> Either T.Text a
gsDecode = gsDecodeG (allFieldNames (Proxy :: Proxy a))

-- | Decode hashmap to a record using generic-sop.
gsDecodeG ::
    forall a xs. (Generic a,  All2 DynamoEncodable (Code a), Code a ~ '[ xs ])
  => [T.Text] -> HMap.HashMap T.Text AttributeValue -> Either T.Text a
gsDecodeG names attrs =
    let Just pairs = fromList $ map (\k -> (k, k `HMap.lookup` attrs)) names
    in to . SOP . Z <$> hsequence (hcliftA dproxy (decodeWithErr . unK) pairs)
  where
    dproxy = Proxy :: Proxy DynamoEncodable
    decodeWithErr (k, Nothing) = first (("Error decoding empty attr: " <> k <> " -> ") <>) (dDecodeEither Nothing)
    decodeWithErr (k, Just attr) = 
        first
          (\err -> "Error decoding attr: " <> k <> " -> " <> err <> " from: " <> T.pack (show attr))
          (dDecodeEither (Just attr))

defaultPutItem :: forall a r. DynamoTable a r => a -> D.PutItem
defaultPutItem item = D.putItem tblname & D.piItem .~ gsEncode item
  where
    tblname = tableName (Proxy :: Proxy a)

defaultCreateTable :: forall a v hash rest. (DynamoTable a 'NoRange, Code a ~ '[ hash ': rest ], DynamoScalar v hash)
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (tableName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    hashname = head (allFieldNames p)
    hashKey = keySchemaElement hashname D.Hash
    keyDefs = [D.attributeDefinition hashname (dType (Proxy :: Proxy hash))]

defaultCreateTableRange :: forall a hash range rest v1 v2.
    (DynamoTable a 'WithRange, Code a ~ '[ hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range)
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTableRange p thr =
    D.createTable (tableName p) (hashKey :| [rangeKey]) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (hashname:rangename:_) = allFieldNames p
    hashKey = keySchemaElement hashname D.Hash
    rangeKey = keySchemaElement rangename D.Range
    keyDefs = [D.attributeDefinition hashname (dType (Proxy :: Proxy hash)),
               D.attributeDefinition rangename (dType (Proxy :: Proxy range))]

defaultQueryKey :: (CanQuery a t hash range, DynamoScalar v1 hash, DynamoScalar v2 range)
    => Proxy a -> hash -> Maybe (RangeOper range) -> D.Query
defaultQueryKey p key Nothing =
  D.query (qTableName p) & D.qKeyConditionExpression .~ Just "#K = :key"
                         & D.qExpressionAttributeNames .~ HMap.singleton "#K" hashname
                         & D.qExpressionAttributeValues .~ HMap.singleton ":key" (dScalarEncode key)
                         & D.qIndexName .~ qIndexName p
  where
    (hashname:_) = allFieldNames p
defaultQueryKey p key (Just range) =
  D.query (qTableName p) & D.qKeyConditionExpression .~ Just condExpression
                         & D.qExpressionAttributeNames .~ attrnames
                         & D.qExpressionAttributeValues .~ attrvals
                         & D.qIndexName .~ qIndexName p
  where
    rangeSubst = "#R"
    condExpression = "#K = :key AND " <> rangeOper range rangeSubst
    attrnames = HMap.fromList [("#K", hashname), (rangeSubst, rangename)]
    attrvals = HMap.fromList $ rangeData range ++ [(":key", dScalarEncode key)]
    (hashname:rangename:_) = allFieldNames p

defaultCreateGlobalIndex :: forall a r parent r2 hash rest v.
  (DynamoIndex a parent r, DynamoTable parent r2, Code a ~ '[hash ': rest ], DynamoScalar v hash)
  => Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
defaultCreateGlobalIndex p thr =
    (globalSecondaryIndex (indexName p) keyschema proj thr, attrdefs)
  where
    (hashname:_) = allFieldNames p
    attrdefs = [D.attributeDefinition hashname (dType (Proxy :: Proxy hash))]
    keyschema = keySchemaElement hashname D.Hash :| []
    proj | Just lst <- nonEmpty attrlist =
                    D.projection & D.pProjectionType .~ Just D.PTInclude
                                 & D.pNonKeyAttributes .~ Just lst
         | otherwise = D.projection & D.pProjectionType .~ Just D.PTKeysOnly
    parentKey = primaryFields (Proxy :: Proxy parent)
    attrlist = filter (`notElem` (parentKey ++ [hashname])) $ allFieldNames (Proxy :: Proxy a)

mkIndexHelper :: forall a parent r2 hash rest range v1 v2.
  (DynamoIndex a parent 'WithRange, DynamoTable parent r2, Code a ~ '[hash ': range ': rest ],
    DynamoScalar v1 hash, DynamoScalar v2 range) =>
  Proxy a -> (NonEmpty D.KeySchemaElement, D.Projection, [D.AttributeDefinition])
mkIndexHelper p = (keyschema, proj, attrdefs)
  where
    (hashname:rangename:_) = allFieldNames p
    (hashproxy, rangeproxy) = (Proxy :: Proxy hash, Proxy :: Proxy range)
    attrdefs = [D.attributeDefinition hashname (dType hashproxy), D.attributeDefinition rangename (dType rangeproxy)]
    --
    keyschema = keySchemaElement hashname D.Hash :| [keySchemaElement rangename D.Range]
    --
    proj | Just lst <- nonEmpty attrlist =
                    D.projection & D.pProjectionType .~ Just D.PTInclude
                                 & D.pNonKeyAttributes .~ Just lst
         | otherwise = D.projection & D.pProjectionType .~ Just D.PTKeysOnly
    parentKey = primaryFields (Proxy :: Proxy parent)
    attrlist = filter (`notElem` (parentKey ++ [hashname, rangename])) $ allFieldNames (Proxy :: Proxy a)

defaultCreateGlobalIndexRange :: forall a parent r2 hash rest range v1 v2.
  (DynamoIndex a parent 'WithRange, DynamoTable parent r2,
    Code a ~ '[hash ': range ': rest ], DynamoScalar v1 hash, DynamoScalar v2 range) =>
  Proxy a -> ProvisionedThroughput -> (D.GlobalSecondaryIndex, [D.AttributeDefinition])
defaultCreateGlobalIndexRange p thr =
    (globalSecondaryIndex (indexName p) keyschema proj thr, attrdefs)
  where
    (keyschema, proj, attrdefs) = mkIndexHelper p

createLocalIndex :: forall a parent r2 hash rest range v1 v2.
  (DynamoIndex a parent 'WithRange, DynamoTable parent r2, Code a ~ '[hash ': range ': rest ],
    DynamoScalar v1 hash, DynamoScalar v2 range) =>
  Proxy a -> (D.LocalSecondaryIndex, [D.AttributeDefinition])
createLocalIndex p =
    (D.localSecondaryIndex (indexName p) keyschema proj, attrdefs)
  where
    (keyschema, proj, attrdefs) = mkIndexHelper p


defaultScan :: (TableScan a r t) => Proxy a -> D.Scan
defaultScan p = D.scan (qsTableName p) & D.sIndexName .~ qsIndexName p
