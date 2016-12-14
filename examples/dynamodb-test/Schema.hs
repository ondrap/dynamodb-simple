{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE TypeSynonymInstances  #-}

-- {-# LANGUAGE DuplicateRecordFields #-}

module Schema where

import           Data.Scientific            (toBoundedInteger)
import qualified Data.Set                   as Set
import           Data.Tagged
import qualified Data.Text                  as T
import           Data.Time.Clock            (UTCTime)
import           Data.Time.Clock.POSIX      (POSIXTime, posixSecondsToUTCTime,
                                             utcTimeToPOSIXSeconds)
import           Data.UUID                  (UUID)
import qualified Data.UUID                  as UUID
import qualified Network.AWS.DynamoDB.Types as D

import           Database.DynamoDB.TH
import           Database.DynamoDB.Types

-- Haskell datatype instances
instance DynamoEncodable UUID where
  dEncode uuid = dEncode (UUID.toText uuid)
  dDecode attr = attr >>= dDecode . Just >>= UUID.fromText
instance DynamoScalar 'D.S UUID where
  scalarEncode = ScS . UUID.toText
  scalarDecode (ScS txt) = UUID.fromText txt

instance DynamoEncodable UTCTime where
  dEncode time = dEncode (truncate (utcTimeToPOSIXSeconds time) :: Int)
  dDecode attr = dDecode attr >>= pure . posixSecondsToUTCTime . (fromIntegral :: Int -> POSIXTime)
instance DynamoScalar 'D.N UTCTime where
  scalarEncode time = ScN (fromIntegral (truncate (utcTimeToPOSIXSeconds time) :: Int))
  scalarDecode (ScN num)= toBoundedInteger num >>= pure . posixSecondsToUTCTime . (fromIntegral :: Int -> POSIXTime)

-- Custom data

data Tag = Red | Green | Blue | White | Black
  deriving (Eq, Ord, Show, Read)
instance DynamoScalar 'D.S Tag
instance DynamoEncodable Tag

data Gender = Male | Female
  deriving (Eq, Ord, Show, Read)
instance DynamoScalar 'D.S Gender
instance DynamoEncodable Gender


data TArticleUUID
type ArticleUUID = Tagged TArticleUUID UUID

data TAuthorUUID
type AuthorUUID = Tagged TAuthorUUID UUID

data TCategory
type Category = Tagged TCategory T.Text

data Author = Author {
    autUuid      :: AuthorUUID
  , autFirstName :: T.Text
  , autLastName  :: T.Text
  , autGender    :: Gender
} deriving (Show)
deriveCollection ''Author defaultTranslate

data Article = Article {
    artUuid       :: ArticleUUID
  , artTitle      :: T.Text
  , artCategory   :: Category
  , artPublished  :: Maybe UTCTime
  , artAuthorUuid :: AuthorUUID
  , artAuthor     :: Author
  , artCoauthor   :: Maybe Author
  , artTags       :: Set.Set Tag
} deriving (Show)

data ArticleIndex = ArticleIndex {
    i_artCategory  :: Category
  , i_artPublished :: UTCTime -- We are using sparse index
  , i_artTitle     :: T.Text
  , i_artAuthor    :: Author
  , i_artTags      :: Set.Set Tag
  , i_artUuid      :: ArticleUUID
} deriving (Show)

data AuthorIndex = AuthorIndex {
    a_artAuthorUuid :: AuthorUUID
  , a_artPublished  :: UTCTime
  , a_artTitle      :: T.Text
  , a_artTags       :: Set.Set Tag
  , a_artUuid      :: ArticleUUID
} deriving (Show)

mkTableDefs "migrateTables" (tableConfig (''Article, NoRange)
                                         [(''ArticleIndex, WithRange), (''AuthorIndex, WithRange)]
                                         [])
