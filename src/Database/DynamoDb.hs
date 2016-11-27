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

module Database.DynamoDb (
    DynamoCollection(..)
  , DynamoTable(..)
  , NoRange, WithRange
) where

import           Control.Lens                     ((.~))
import           Data.Function                    ((&))
import qualified Data.HashMap.Strict              as HMap
import           Data.List.NonEmpty               (NonEmpty((:|)))
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

-- | Basic instance for dynamo collection (table or index)
-- This instances fixes the tableName and the sort key
class (Generic a, HasDatatypeInfo a) => DynamoCollection a r | a -> r where
  -- | Dynamo table/index name; default is the constructor name
  tableName :: Proxy a -> T.Text
  default tableName :: (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

-- | Descritpion of dynamo table
class (DynamoCollection a r, Generic a, HasDatatypeInfo a) => DynamoTable a r | a -> r where
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

type family PrimaryKey (a :: [[k]]) r :: *
type instance PrimaryKey ('[ key ': range ': rest ] ) WithRange = (key, range)
type instance PrimaryKey ('[ key ': rest ]) NoRange = key

class DeleteItem a r where
  iDeleteItem :: (DynamoTable a r, RecordOK (Code a) r, Code a ~ '[ hash ': range ': xss ])
            => Proxy r -> Proxy a -> PrimaryKey (Code a) r -> D.DeleteItem
instance DeleteItem a NoRange where
  iDeleteItem _ p key =
        D.deleteItem (tableName p) & D.diKey .~ HMap.singleton (fst $ gdHashField p) (dEncode key)
instance DeleteItem a WithRange where
  iDeleteItem _ p (key, range) = D.deleteItem (tableName p) & D.diKey .~ HMap.fromList plist
    where
      plist = [(fst $ gdHashField p, dEncode key), (fst $ gdRangeField p, dEncode range)]

class TableCreate a r where
  iCreateTable :: (DynamoTable a r, RecordOK (Code a) r, Code a ~ '[ hash ': range ': xss ])
                           => Proxy r -> Proxy a -> ProvisionedThroughput -> D.CreateTable
instance TableCreate a NoRange where
  iCreateTable _ = defaultCreateTable
instance TableCreate a WithRange where
  iCreateTable _ = defaultCreateTableRange

class DynamoCollection a r => TableQuery a r where
  type QueryRange (b :: [[k]]) r :: *

  queryKey :: (Code a ~ '[ key ': rest ], DynamoScalar key) => Proxy a -> key -> D.Query
  queryKey = defaultQueryKey

  queryKeyRange ::
        (TableQuery a r, Code a ~ '[ key ': hash ': rest ], RecordOK (Code a) r)
      => Proxy a -> QueryRange (Code a) r -> D.Query

instance (DynamoCollection a NoRange, Code a ~ '[ xs ]) => TableQuery a NoRange where
  type QueryRange ('[ key ': rest ]) NoRange  = key
  queryKeyRange = defaultQueryKey

instance  (DynamoCollection a WithRange, Code a ~ '[ xs ]) => TableQuery a WithRange where
  type QueryRange ('[ key ': range ': rest ] ) WithRange  = (key, RangeOper range)
  queryKeyRange = defaultQueryKeyRange

type family RecordOK (a :: [[k]]) r :: Constraint
type instance RecordOK '[ hash ': rest ] NoRange = (DynamoScalar hash)
type instance RecordOK '[ hash ': range ': rest ] WithRange = (DynamoScalar hash, DynamoScalar range)


class DynamoIndex a t r | a -> t, a -> r where
  createIndex :: Proxy a -> ProvisionedThroughput -> D.GlobalSecondaryIndex
  -- createIndex = defaultCreateIndex

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

defaultPutItem ::
     (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a), DynamoTable a r)
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

defaultQueryKey :: (Code a ~ '[ key ': rest ], DynamoCollection a r, DynamoScalar key)
  => Proxy a -> key -> D.Query
defaultQueryKey p key =
  D.query (tableName p) & D.qKeyConditionExpression .~ Just "#K = :key"
                        & D.qExpressionAttributeNames .~ HMap.singleton "K" hashname
                        & D.qExpressionAttributeValues .~ HMap.singleton "key" (dEncode key)
  where
    (hashname, _) = gdHashField p

defaultQueryKeyRange :: (Code a ~ '[ hash ': range ': rest ], DynamoCollection a r,
                          RecordOK (Code a) WithRange)
  => Proxy a -> (hash, RangeOper range) -> D.Query
defaultQueryKeyRange p (key, range) =
  D.query (tableName p) & D.qKeyConditionExpression .~ Just condExpression
                        & D.qExpressionAttributeNames .~ attrnames
                        & D.qExpressionAttributeValues .~ attrvals
  where
    rangeSubst = "R"
    condExpression = "#K = :key AND <> " <> rangeOper range rangeSubst
    attrnames = HMap.fromList [("K", hashname), (rangeSubst, rangename)]
    attrvals = HMap.fromList $ [("key", dEncode key)] ++ rangeData range
    (hashname, _) = gdHashField p
    (rangename, _) = gdRangeField p

-- defaultCeateIndex ::
--   (DynamoIndex a t r) =>
--   Proxy a -> ProvisionedThroughput -> D.GlobalSecondaryIndex
-- defaultCreateIndex p thr =
--     globalSecondaryIndex (tableName p) schema proj thr
--   where
--     schema =
--     proj =
