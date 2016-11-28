{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE EmptyDataDecls #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE ScopedTypeVariables #-}

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
  , treti :: T.Text
  , ctvrty :: Maybe T.Text
  , paty :: BS.ByteString
} deriving (Show, GHC.Generic)
instance Generic Test
instance HasDatatypeInfo Test
instance DynamoCollection Test WithRange IsTable
instance DynamoTable Test WithRange IsTable

data TestIndex = TestIndex {
    i_treti :: T.Text
  , i_paty :: BS.ByteString
  , i_druhy :: T.Text
} deriving (Show, GHC.Generic)
instance Generic TestIndex
instance HasDatatypeInfo TestIndex
instance DynamoCollection TestIndex NoRange IsIndex
instance DynamoIndex TestIndex Test NoRange IsIndex

data P_Prvni
instance InCollection P_Prvni Test
instance ColumnInfo P_Prvni where
  columnName _ = "prvni"
data P_Druhy
instance InCollection P_Druhy Test
instance InCollection P_Druhy TestIndex
instance ColumnInfo P_Druhy where
  columnName _ = "druhy"
data P_Treti
instance InCollection P_Treti Test
instance InCollection P_Treti TestIndex
instance ColumnInfo P_Treti where
  columnName _ = "treti"

pattern ColPrvni = Column :: Column Int TypColumn P_Prvni
pattern ColDruhy = Column :: Column T.Text TypColumn P_Druhy
pattern ColTreti = Column :: Column T.Text TypColumn P_Treti

x :: FilterCondition TestIndex
x = (ColTreti ==. "blabla") &&. (ColDruhy >. "test")

test :: IO ()
test = print (dumpCondition x)
