{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TemplateHaskell #-}

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
import Database.DynamoDb.TH

data Test = Test {
    prvni :: Int
  , druhy :: T.Text
  , treti :: T.Text
  , ctvrty :: Maybe T.Text
  , paty :: BS.ByteString
} deriving (Show, GHC.Generic)

data TestIndex = TestIndex {
    i_treti :: T.Text
  , i_paty :: BS.ByteString
  , i_druhy :: Int
} deriving (Show, GHC.Generic)

$(mkTableDefs (''Test, True) [(''TestIndex, False)])

x :: FilterCondition TestIndex
x = (colTreti ==. "blabla") &&. (colDruhy >. "test")

test :: IO ()
test = print (dumpCondition x)
