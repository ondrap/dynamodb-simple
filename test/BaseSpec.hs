{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE DeriveGeneric         #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}

module BaseSpec where

import           Control.Exception.Safe (catchAny, finally, SomeException, try)
import           Control.Lens           (set)
import           Control.Monad.IO.Class (liftIO)
import           Data.Function          ((&))
import           Data.List              (sort)
import           Data.Proxy
import qualified Data.Text              as T
import qualified GHC.Generics           as GHC
import           Network.AWS
import           Network.AWS.DynamoDB   (dynamoDB, provisionedThroughput)
import           System.Environment     (setEnv)
import           System.IO              (stdout)
import           Test.Hspec
import Data.Either (isLeft)

import           Database.DynamoDB
import           Database.DynamoDB.TH

data Test = Test {
    iHashKey  :: T.Text
  , iRangeKey :: Int
  , iText     :: T.Text
  , iBool     :: Bool
  , iDouble   :: Double
  , iMText    :: Maybe T.Text
} deriving (Show, GHC.Generic, Eq, Ord)
mkTableDefs "migrate" (''Test, WithRange) [] []

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
          migrate (provisionedThroughput 5 5) (provisionedThroughput 5 5)
          code `finally` deleteTable (Proxy :: Proxy Test)

spec :: Spec
spec = do
  describe "Basic operations" $ do
    withDb "putItem/getItem works" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 Nothing
            testitem2 = Test "2" 3 "text" False 4.15 (Just "text")
        putItem testitem1
        putItem testitem2
        it1 <- getItem Strongly ("1", 2)
        it2 <- getItem Strongly ("2", 3)
        liftIO $ Just testitem1 `shouldBe` it1
        liftIO $ Just testitem2 `shouldBe` it2
    withDb "getItemBatch/putItemBatch work" $ do
        let template i = Test (T.pack $ show i) i "text" False 3.14 Nothing
            newItems = map template [1..300]
        putItemBatch newItems
        --
        let keys = map (snd . itemToKey) newItems
        items <- getItemBatch Strongly keys
        liftIO $ sort items `shouldBe` sort newItems
    withDb "insertItem doesn't overwrite items" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 Nothing
            testitem1_ = Test "1" 2 "XXXX" True 3.14 Nothing
        insertItem testitem1
        (res :: Either SomeException ()) <- try (insertItem testitem1_)
        liftIO $ res `shouldSatisfy` isLeft

main :: IO ()
main = hspec spec
