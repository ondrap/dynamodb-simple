{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}

module BaseSpec where

import           Control.Exception.Safe   (SomeException, catchAny, finally,
                                           try)
import           Control.Lens             (set, (.~))
import           Control.Monad.IO.Class   (liftIO)
import           Data.Conduit             (runConduit, (=$=))
import qualified Data.Conduit.List        as CL
import           Data.Either              (isLeft)
import           Data.Function            ((&))
import           Data.List                (sort)
import           Data.Proxy
import qualified Data.Text                as T
import qualified GHC.Generics             as GHC
import           Network.AWS
import           Network.AWS.DynamoDB     (dynamoDB, provisionedThroughput)
import           System.Environment       (setEnv)
import           System.IO                (stdout)
import           Test.Hspec
import Data.Semigroup ((<>))

import           Database.DynamoDB
import           Database.DynamoDB.Filter
import           Database.DynamoDB.TH
import           Database.DynamoDB.Types  (RangeOper (..))
import           Database.DynamoDB.Update

data Test = Test {
    iHashKey  :: T.Text
  , iRangeKey :: Int
  , iText     :: T.Text
  , iBool     :: Bool
  , iDouble   :: Double
  , iInt      :: Int
  , iMText    :: Maybe T.Text
} deriving (Show, GHC.Generic, Eq, Ord)
mkTableDefs "migrateTest" (''Test, WithRange) [] []

withDb :: Example (IO b) => String -> AWS b -> SpecWith (Arg (IO b))
withDb msg code = it msg runcode
  where
    runcode = do
      setEnv "AWS_ACCESS_KEY_ID" "XXXXXXXXXXXXXX"
      setEnv "AWS_SECRET_ACCESS_KEY" "XXXXXXXXXXXXXXfdjdsfjdsfjdskldfs+kl"
      lgr  <- newLogger Info stdout
      env  <- newEnv NorthVirginia Discover
      let dynamo = setEndpoint False "localhost" 8000 dynamoDB
      let newenv = env & configure dynamo
                       & set envLogger lgr
      runResourceT $ runAWS newenv $ do
          deleteTable (Proxy :: Proxy Test) `catchAny` (\_ -> return ())
          migrateTest (provisionedThroughput 5 5) (provisionedThroughput 5 5)
          code `finally` deleteTable (Proxy :: Proxy Test)

spec :: Spec
spec = do
  describe "Basic operations" $ do
    withDb "putItem/getItem works" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 2 Nothing
            testitem2 = Test "2" 3 "text" False 4.15 3 (Just "text")
        putItem testitem1
        putItem testitem2
        it1 <- getItem Strongly ("1", 2)
        it2 <- getItem Strongly ("2", 3)
        liftIO $ Just testitem1 `shouldBe` it1
        liftIO $ Just testitem2 `shouldBe` it2
    withDb "getItemBatch/putItemBatch work" $ do
        let template i = Test (T.pack $ show i) i "text" False 3.14 i Nothing
            newItems = map template [1..300]
        putItemBatch newItems
        --
        let keys = map (snd . itemToKey) newItems
        items <- getItemBatch Strongly keys
        liftIO $ sort items `shouldBe` sort newItems
    withDb "insertItem doesn't overwrite items" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 2 Nothing
            testitem1_ = Test "1" 2 "XXXX" True 3.14 3 Nothing
        insertItem testitem1
        (res :: Either SomeException ()) <- try (insertItem testitem1_)
        liftIO $ res `shouldSatisfy` isLeft
    withDb "scanSource works correctly with sLimit" $ do
        let template i = Test (T.pack $ show i) i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        let squery = scanOpts & sFilterCondition .~ Just (colIInt >. 50)
                              & sLimit .~ Just 1
        res <- runConduit $ scanSource squery =$= CL.consume
        liftIO $ res `shouldBe` drop 50 newItems
    withDb "querySource works correctly with qLimit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        let squery = queryOpts "hashkey" & qFilterCondition .~ Just (colIInt >. 50)
                                         & qLimit .~ Just 1
        res <- runConduit $ querySource squery =$= CL.consume
        liftIO $ res `shouldBe` drop 50 newItems
    withDb "queryCond works correctly with qLimit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- queryCond "hashkey" Nothing (colIInt >. 50) Forward 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "scanCond works correctly with qLimit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- scanCond (colIInt >. 50) 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "querySimple works correctly with RangeOper" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- querySimple "hashkey" (Just $ RangeLessThan 30) Backward 5
        liftIO $ map iRangeKey items `shouldBe` [29,28..25]
    withDb "queryCond works correctly with -1 limit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        (items :: [Test]) <- queryCond "hashkey" Nothing (colIInt >. 50) Backward (-1)
        liftIO $ items `shouldBe` []
    withDb "updateItemByKey works" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 2 (Just "something")
        putItem testitem1
        new1 <- updateItemByKey (itemToKey testitem1)
                                ((colIInt +=. 5) <> (colIText =. "updated") <> (colIMText =. Nothing))
        Just new2 <- getItem Strongly (snd (itemToKey testitem1))
        liftIO $ do
            new1 `shouldBe` new2
            iInt new1 `shouldBe` 7
            iText new1 `shouldBe` "updated"
            iMText new1 `shouldBe` Nothing
    -- withDb "scan continuation works" $ do
    --     let template i = Test "hashkey" i "text" False 3.14 i Nothing
    --         newItems = map template [1..55]
    --     putItemBatch newItems



main :: IO ()
main = hspec spec
