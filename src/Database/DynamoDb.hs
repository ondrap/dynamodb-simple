{-# LANGUAGE DataKinds              #-}
{-# LANGUAGE DeriveGeneric          #-}
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

module Database.DynamoDb where

import           Control.Lens                     ((.~))
import           Data.Function                    ((&))
import qualified Data.HashMap.Strict              as HMap
import qualified Data.Text                        as T
import           Generics.SOP
import qualified GHC.Generics                     as GHC
import qualified Network.AWS.DynamoDB.CreateTable as D
import qualified Network.AWS.DynamoDB.DeleteItem  as D
import Data.Monoid ((<>))
import qualified Network.AWS.DynamoDB.Query  as D
import qualified Network.AWS.DynamoDB.PutItem     as D
import           Network.AWS.DynamoDB.Types       (AttributeValue,
                                                   ProvisionedThroughput, keySchemaElement)
import qualified Network.AWS.DynamoDB.Types       as D
import Data.List.NonEmpty (NonEmpty((:|)))
import GHC.Exts (Constraint)

import Database.DynamoDb.Types

-- genTableWithRange ''Test [keyIndex ''DIndex, rangeIndex 'PIndex]

class (Generic a, HasDatatypeInfo a) => DynamoTable a r | a -> r where
  tableName :: Proxy a -> T.Text -- TODO: default podle typu
  default tableName :: (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

  putItem :: a -> D.PutItem
  default putItem :: (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a)) => a -> D.PutItem
  putItem = defaultPutItem

  createTable :: Proxy a -> ProvisionedThroughput -> D.CreateTable
  default createTable :: (TableCreate r a, Generic a, HasDatatypeInfo a, RecordOK r (Code a),
                          Code a ~ '[ hash ': range ': rest ])
                           => Proxy a -> ProvisionedThroughput -> D.CreateTable
  createTable = iCreateTable (Proxy :: Proxy r)

  queryKey :: (Code a ~ '[ key ': rest ], DynamoScalar key) => Proxy a -> key -> D.Query
  queryKey = defaultQueryKey

  queryKeyRange :: (TableQuery r a, Code a ~ '[ key ': hash ': rest ], RecordOK r (Code a))
      => Proxy a -> QueryRange r (Code a) -> D.Query
  queryKeyRange = iQueryKeyRange (Proxy :: Proxy r)

type family QueryRange r (a :: [[k]]) :: *
type instance QueryRange WithRange ('[ key ': range ': rest ] )  = (key, RangeOper range)
type instance QueryRange NoRange ('[ key ': rest ])  = key

class TableCreate r a where
  iCreateTable :: (DynamoTable a r, RecordOK r (Code a), Code a ~ '[ hash ': range ': xss ])
                           => Proxy r -> Proxy a -> ProvisionedThroughput -> D.CreateTable
instance TableCreate NoRange a where
  iCreateTable _ = defaultCreateTable
instance TableCreate WithRange a where
  iCreateTable _ = defaultCreateTableRange

class TableQuery r a where
  iQueryKeyRange :: (DynamoTable a r, RecordOK r (Code a), Code a ~ '[ hash ': range ': xss ])
    => Proxy r -> Proxy a -> QueryRange r (Code a) -> D.Query
instance TableQuery NoRange a where
  iQueryKeyRange _ = defaultQueryKey
instance TableQuery WithRange a where
  iQueryKeyRange _ = defaultQueryKeyRange

data NoRange
data WithRange

type family RecordOK r (a :: [[k]]) :: Constraint
type instance RecordOK NoRange '[ hash ': rest ] = (DynamoScalar hash)
type instance RecordOK WithRange '[ hash ': range ': rest ] = (DynamoScalar hash, DynamoScalar range)

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

gdHashField :: forall a hash rest. (Generic a, HasDatatypeInfo a, Code a ~ '[ hash ': rest ] )
  => Proxy a -> (T.Text, Proxy hash)
gdHashField _ =
  case datatypeInfo (Proxy :: Proxy a) of
    ADT _ _ cs -> head $ hcollapse $ hliftA getName cs
    Newtype _ _ c -> head $ hcollapse $ hliftA getName (c :* Nil)
  where
    getName :: ConstructorInfo xs -> K (T.Text, Proxy hash) xs
    getName (Record _ fields) =
        K $ (, Proxy) . head $ hcollapse $ hliftA (\(FieldInfo name) -> K (T.pack name)) fields
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
        K $ (, Proxy) . (!! 1) $ hcollapse $ hliftA (\(FieldInfo name) -> K (T.pack name)) fields
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

defaultCreateTable :: (Generic a, HasDatatypeInfo a, RecordOK NoRange (Code a), Code a ~ '[ hash ': rest ])
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (gdConstrName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (firstname, firstproxy) = gdHashField p
    hashKey = keySchemaElement firstname D.Hash
    keyDefs = [D.attributeDefinition firstname (dType firstproxy)]

defaultCreateTableRange ::
    (Generic a, HasDatatypeInfo a, RecordOK WithRange (Code a), Code a ~ '[ hash ': range ': rest ])
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

defaultQueryKey :: (Code a ~ '[ key ': rest ], DynamoTable a k, DynamoScalar key)
  => Proxy a -> key -> D.Query
defaultQueryKey p key =
  D.query (tableName p) & D.qKeyConditionExpression .~ Just "#K = :key"
                        & D.qExpressionAttributeNames .~ HMap.singleton "K" hashname
                        & D.qExpressionAttributeValues .~ HMap.singleton "key" (dEncode key)
  where
    (hashname, _) = gdHashField p

defaultQueryKeyRange :: (Code a ~ '[ hash ': range ': rest ], DynamoTable a k,
                          RecordOK WithRange (Code a))
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

data Test = Test {
    prvni :: Int
  , druhy :: T.Text
} deriving (Show, GHC.Generic)
instance Generic Test
instance HasDatatypeInfo Test
instance DynamoTable Test NoRange where
