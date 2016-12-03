{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE EmptyDataDecls             #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeSynonymInstances       #-}

-- {-# LANGUAGE DuplicateRecordFields #-}

module Main where
import           Control.Lens               (Iso', iso, set, (%~), (.~), (^.))
import           Control.Monad              (forM_)
import           Control.Monad.IO.Class     (MonadIO, liftIO)
import           Data.Conduit               (runConduit, (=$=))
import qualified Data.Conduit.List          as CL
import           Data.Function              ((&))
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.List.NonEmpty
import           Data.Proxy
import           Data.Scientific            (toBoundedInteger)
import qualified Data.Set                   as Set
import           Data.Tagged
import qualified Data.Text                  as T
import qualified Data.Text.IO               as T
import           Data.Time.Clock            (NominalDiffTime, UTCTime,
                                             addUTCTime, getCurrentTime)
import           Data.Time.Clock.POSIX      (POSIXTime, posixSecondsToUTCTime,
                                             utcTimeToPOSIXSeconds)
import           Data.UUID                  (UUID)
import qualified Data.UUID                  as UUID
import           Data.UUID.V4               (nextRandom)
import qualified GHC.Generics               as GHC
import           Network.AWS
import           Network.AWS.DynamoDB       (dynamoDB, provisionedThroughput)
import qualified Network.AWS.DynamoDB.Types as D
import           System.Environment         (setEnv)
import           System.IO                  (stdout)

import           Database.DynamoDB
import           Database.DynamoDB.Filter
import           Database.DynamoDB.TH
import           Database.DynamoDB.Types
import           Database.DynamoDB.Update


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
} deriving (Show, GHC.Generic)
$(deriveCollection ''Author)

data Article = Article {
    artUuid       :: ArticleUUID
  , artTitle      :: T.Text
  , artCategory   :: Category
  , artPublished  :: Maybe UTCTime
  , artAuthorUuid :: AuthorUUID
  , artAuthor     :: Author
  , artCoauthor   :: Maybe Author
  , artTags       :: Set.Set Tag
} deriving (Show, GHC.Generic)

data ArticleIndex = ArticleIndex {
    i_artCategory  :: Category
  , i_artPublished :: UTCTime -- We are using sparse index
  , i_artTitle     :: T.Text
  , i_artAuthor    :: Author
  , i_artTags      :: Set.Set Tag
} deriving (Show, GHC.Generic)

data AuthorIndex = AuthorIndex {
    a_artAuthorUuid :: AuthorUUID
  , a_artPublished  :: UTCTime
  , a_artTitle      :: T.Text
  , a_artTags       :: Set.Set Tag
} deriving (Show, GHC.Generic)

$(mkTableDefs "migrateTables" (''Article, NoRange) [(''ArticleIndex, WithRange), (''AuthorIndex, WithRange)])

logmsg :: MonadIO m => T.Text -> m ()
logmsg = liftIO . T.putStrLn

genArticles :: forall m. MonadIO m => m [Article]
genArticles = do
    authuuid <- Tagged <$> liftIO nextRandom
    let author1 = Author authuuid "John" "Doe" Male
        author2 = Author authuuid "Bill" "Clinton" Male
        author3 = Author authuuid "Barack" "Obama" Male
    sequence [
        mkArticle author1 Nothing "Title 1" (Tagged "News") (Set.singleton Red) (Just $ (-1))
      , mkArticle author1 (Just author2) "Title 2" (Tagged "News") (Set.singleton Blue) (Just $ (-2))
      , mkArticle author1 Nothing "Title 3" (Tagged "Comedy") (Set.fromList [Red, Blue]) (Just $ (-200))
      , mkArticle author1 (Just author3) "Title 4" (Tagged "Comedy") (Set.fromList [Red, Blue]) Nothing
      ]
  where
    day :: NominalDiffTime
    day = 24 * 3600
    mkArticle :: Author -> Maybe Author -> T.Text -> Category -> Set.Set Tag -> Maybe NominalDiffTime-> m Article
    mkArticle author coauthor title category tags mdays = do
      now <- liftIO getCurrentTime
      artuuid <- Tagged <$> liftIO nextRandom
      let published = (\days -> (day * days) `addUTCTime` now) <$> mdays
      return $ Article artuuid title category published (autUuid author) author coauthor tags

withLog :: MonadIO m => T.Text -> m () -> m ()
withLog msg code = do
  logmsg "---------------------------------------"
  logmsg msg
  logmsg "---------------------------------------"
  code
  logmsg ""

main :: IO ()
main = do
  -- We don't need this for accessing local dynamodb instance, but newEnv complains if the environment is not set
  setEnv "AWS_ACCESS_KEY_ID" "XXXXXXXXXXXXXX"
  setEnv "AWS_SECRET_ACCESS_KEY" "XXXXXXXXXXXXXXfdjdsfjdsfjdskldfs+kl"

  lgr  <- newLogger Info stdout
  env  <- newEnv NorthVirginia Discover
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  let newenv = env & configure dynamo
                   & set envLogger lgr
  runResourceT $ runAWS newenv $ do
      -- Create tables; provisionedThroughput for indexes is some low default
      migrateTables (provisionedThroughput 5 5) []

      withLog "Entring some data" $
        genArticles >>= putItemBatch

      withLog "Querying published news articles" $ do
        (items :: [ArticleIndex]) <- querySimple (Tagged "News") Nothing Forward 10
        forM_ items (liftIO . print)

      withLog "Querying published comedy articles with condition" $ do
        (items :: [ArticleIndex]) <- querySimple (Tagged "Comedy") Nothing Backward 10
        forM_ items (liftIO . print)

      -- Scan
      -- Update
      -- Delete
      
