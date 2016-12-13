{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- {-# LANGUAGE DuplicateRecordFields #-}

module Main where

import           Control.Exception.Safe   (catchAny)
import           Control.Lens             (set)
import           Control.Monad            (forM_)
import           Control.Monad.IO.Class   (MonadIO, liftIO)
import           Data.Bool                (bool)
import           Data.Function            ((&))
import           Data.Monoid              ((<>))
import           Data.Proxy
import qualified Data.Set                 as Set
import           Data.Tagged
import qualified Data.Text                as T
import qualified Data.Text.IO             as T
import           Data.Time.Clock          (NominalDiffTime, addUTCTime,
                                           getCurrentTime)
import           Data.UUID.V4             (nextRandom)
import           Network.AWS
import           Network.AWS.DynamoDB     (dynamoDB, provisionedThroughput)
import           System.Environment       (setEnv)
import           System.IO                (stdout)

import           Database.DynamoDB
import           Database.DynamoDB.Filter
import           Database.DynamoDB.Update

import           Schema

main :: IO ()
main = do
  -- We don't need this for accessing local dynamodb instance, but newEnv complains if the environment is not set
  setEnv "AWS_ACCESS_KEY_ID" "XXXXXXXXXXXXXX"
  setEnv "AWS_SECRET_ACCESS_KEY" "XXXXXXXXXXXXXXfdjdsfjdsfjdskldfs+kl"

  lgr  <- newLogger Info stdout
  env  <- newEnv Discover
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  let newenv = env & configure dynamo
                   & set envLogger lgr
  runResourceT $ runAWS newenv $ do
      -- Delete table
      deleteTable (Proxy :: Proxy Article) `catchAny` (\_ -> return ())
      -- Create table; provisionedThroughput for indexes is some low default
      migrateTables mempty Nothing

      withLog "Loading data" $
        genArticles >>= putItemBatch

      withLog "Querying published news articles" $ do
        items <- querySimple iArticleIndex (Tagged "News") Nothing Backward 5
        forM_ items (liftIO . print)

      withLog "Querying published news articles with red tag" $ do
        items <- queryCond iArticleIndex (Tagged "News") Nothing (artTags' `setContains` Red) Backward 5
        forM_ items (liftIO . print)

      withLog "Querying published news articles from Bill Clinton" $ do
        let condition = (artAuthor' <.> autFirstName' ==. "Bill" &&. artAuthor' <.> autLastName' ==. "Clinton" )
        items <- queryCond iArticleIndex (Tagged "News") Nothing condition Backward 10
        forM_ items (liftIO . print)

      -- Scan with condition
      withLog "Simple scan unpublished articles" $ do
        items <- scanCond tArticle (artPublished' ==. Nothing) 10
        forM_ items (liftIO . print)

      -- Update
      withLog "Change field in a nested structure with Maybe" $ do
        -- get some article
        [item] <- scanCond tArticle (artCoauthor' /=. Nothing) 1
        logmsg $ "Before update: " <> T.pack (show item)
        newitem <- updateItemByKey tArticle (tableKey item) (artCoauthor' <.> autGender' =. Female)
        logmsg $ "After update: " <> T.pack (show newitem)

      -- Delete


logmsg :: MonadIO m => T.Text -> m ()
logmsg = liftIO . T.putStrLn

genArticles :: forall m. MonadIO m => m [Article]
genArticles = do
    authuuid <- Tagged <$> liftIO nextRandom
    let author1 = Author authuuid "John" "Doe" Male
        author2 = Author authuuid "Bill" "Clinton" Male
        author3 = Author authuuid "Barack" "Obama" Male
    news <- mapM mkNews $ zip3 (cycle [author1, author2, author3])
                               (cycle [Just author3, Nothing, Just author2, Nothing, Just author1])
                                (zip [1..(1000 :: Int)] (cycle [Set.singleton Red, Set.singleton Blue,
                                                        Set.fromList [Red, Green], Set.fromList [Red, Green, Blue]]))


    comedy <- mapM mkComedy $ zip3 (cycle [author1, author2, author3])
                               (cycle [Just author3, Nothing, Just author2, Nothing, Just author1])
                                (zip [1..(1000 :: Int)] (cycle [Set.empty, Set.singleton Green,
                                                        Set.fromList [Blue, Green], Set.fromList [Red, Green, Blue]]))

    return $ news ++ comedy
  where
    mkNews (author, coauthor, (i, tags)) =
      mkArticle author coauthor ("News title " <> T.pack (show i))
                (Tagged "News") tags (bool (Just $ fromIntegral (-i)) Nothing (odd i))
    mkComedy (author, coauthor, (i, tags)) =
      mkArticle author coauthor ("Comedy title " <> T.pack (show i))
                (Tagged "Comedy") tags (bool (Just $ fromIntegral (-i)) Nothing (even i))

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
