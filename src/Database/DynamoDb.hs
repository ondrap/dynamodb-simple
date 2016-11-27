{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.DynamoDb (
    DynamoCollection(..)
  , DynamoTable(..)
  , DynamoIndex(..)
  , queryKey, queryKeyRange
  , NoRange, WithRange
  , IsTable, IsIndex
) where

import Data.Proxy
import           Network.AWS.DynamoDB.Types       (AttributeValue,
                                                   ProvisionedThroughput, keySchemaElement,
                                                   globalSecondaryIndex, provisionedThroughput)
import qualified Data.Text as T
import           Generics.SOP
import qualified GHC.Generics                     as GHC
import qualified Data.ByteString as BS
import qualified Network.AWS.DynamoDB.Types       as D

import Database.DynamoDb.Class
import Database.DynamoDb.Types
import Database.DynamoDb.Filter

data Test = Test {
    prvni :: Int
  , druhy :: T.Text
  -- , treti :: T.Text
  -- , ctvrty :: Maybe T.Text
  -- , paty :: BS.ByteString
} deriving (Show, GHC.Generic)
instance Generic Test
instance HasDatatypeInfo Test
instance DynamoCollection Test WithRange IsTable
instance DynamoTable Test WithRange IsTable

data TestIndex = TestIndex {
    i_treti :: T.Text
  , i_paty :: BS.ByteString
} deriving (Show, GHC.Generic)
instance Generic TestIndex
instance HasDatatypeInfo TestIndex
instance DynamoCollection TestIndex NoRange IsIndex
instance DynamoIndex TestIndex Test NoRange IsIndex

colPrvni :: Column Test Int TypColumn
colPrvni = Column (ColName "prvni")

colDruhy :: Column Test T.Text TypColumn
colDruhy = Column (ColName "prvni")

x :: FilterCondition Test
x = colPrvni ==. (13 :: Int)

test :: IO ()
test = print x
