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

module Database.DynamoDb where

import           Control.Lens                     ((.~), (^.))
import           Data.Double.Conversion.Text      (toShortest)
import           Data.Function                    ((&))
import           Data.HashMap.Strict              (HashMap)
import qualified Data.HashMap.Strict              as HMap
import qualified Data.Text                        as T
import           Generics.SOP
import qualified GHC.Generics                     as GHC
import qualified Network.AWS.DynamoDB.CreateTable as D
import qualified Network.AWS.DynamoDB.DeleteItem  as D
import qualified Network.AWS.DynamoDB.PutItem     as D
import           Network.AWS.DynamoDB.Types       (AttributeValue,
                                                   ScalarAttributeType,
                                                   attributeValue, ProvisionedThroughput, keySchemaElement)
import qualified Network.AWS.DynamoDB.Types       as D
import           Text.Read                        (readMaybe)
import Data.List.NonEmpty (NonEmpty((:|)))

-- genTableWithRange ''Test [keyIndex ''DIndex, rangeIndex 'PIndex]

class DynamoTable a r | a -> r where
  tableName :: Proxy a -> T.Text -- TODO: default podle typu
  default tableName :: (Generic a, HasDatatypeInfo a, Code a ~ '[ xss ]) => Proxy a -> T.Text
  tableName = gdConstrName

  putItem :: a -> D.PutItem
  default putItem :: (Generic a, HasDatatypeInfo a, All2 DynamoEncodable (Code a)) => a -> D.PutItem
  putItem = defaultPutItem

  createTable :: Proxy a -> ProvisionedThroughput -> D.CreateTable
  default createTable :: (Generic a, HasDatatypeInfo a, Code a ~ '[ fs ': rest ], DynamoScalar fs ) => Proxy a -> ProvisionedThroughput -> D.CreateTable
  createTable = defaultCreateTable
  -- deleteItem :: k -> D.DeleteItem

class DynamoScalar a where
  dType :: Proxy a -> ScalarAttributeType
instance DynamoScalar Int where
  dType _ = D.N
instance DynamoScalar T.Text where
  dType _ = D.S

class DynamoEncodable a where
  dEncode :: a -> AttributeValue
  dDecode :: AttributeValue -> Maybe a

instance DynamoEncodable Int where
  dEncode num = attributeValue & D.avN .~ (Just $ T.pack (show num))
  dDecode attr = attr ^. D.avN >>= readMaybe . T.unpack
instance DynamoEncodable Double where
  dEncode num = attributeValue & D.avN .~ (Just $ toShortest num)
  dDecode attr = attr ^. D.avN >>= readMaybe . T.unpack
instance DynamoEncodable Bool where
  dEncode b = attributeValue & D.avBOOL .~ Just b
  dDecode attr = attr ^. D.avBOOL
instance DynamoEncodable T.Text where
  dEncode t = attributeValue & D.avS .~ Just t
  dDecode attr = attr ^. D.avS

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
    getName :: ConstructorInfo xs -> K (T.Text, Proxy fs) xs
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

defaultCreateTable :: (Generic a, HasDatatypeInfo a, Code a ~ '[ fs ': rest ],
                       DynamoScalar fs )
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
defaultCreateTable p thr =
    D.createTable (gdConstrName p) (hashKey :| []) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (firstname, firstproxy) = gdHashField p
    hashKey = keySchemaElement firstname D.Hash
    keyDefs = [D.attributeDefinition firstname (dType firstproxy)]

createTableRange ::
    (Generic a, HasDatatypeInfo a, Code a ~ '[ key ': range ': rest ], DynamoScalar key, DynamoScalar range)
  => Proxy a -> ProvisionedThroughput -> D.CreateTable
createTableRange p thr =
    D.createTable (gdConstrName p) (hashKey :| [rangeKey]) thr
      & D.ctAttributeDefinitions .~ keyDefs
  where
    (hashname, hashproxy) = gdHashField p
    (rangename, rangeproxy) = gdRangeField p
    hashKey = keySchemaElement hashname D.Hash
    rangeKey = keySchemaElement rangename D.Range
    keyDefs = [D.attributeDefinition hashname (dType hashproxy),
               D.attributeDefinition rangename (dType rangeproxy)]

data Test = Test {
    prvni :: Int
  , druhy :: T.Text
} deriving (Show, GHC.Generic)
instance Generic Test
instance HasDatatypeInfo Test
instance DynamoTable Test () where
  createTable = createTableRange
