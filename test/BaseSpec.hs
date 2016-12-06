{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}

module BaseSpec where

import           Control.Exception.Safe   (SomeException, catchAny, finally,
                                           try)
import           Control.Lens             ((.~))
import           Control.Monad.IO.Class   (liftIO)
import           Data.Conduit             (runConduit, (=$=))
import qualified Data.Conduit.List        as CL
import           Data.Either              (isLeft)
import           Data.Function            ((&))
import           Data.List                (sort)
import           Data.Proxy
import           Data.Semigroup           ((<>))
import qualified Data.Text                as T
import           Network.AWS
import           Network.AWS.DynamoDB     (dynamoDB)
import           System.Environment       (setEnv)
import           System.IO                (stdout)
import           Test.Hspec

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
} deriving (Show, Eq, Ord)
mkTableDefs "migrateTest" (tableConfig (''Test, WithRange) [] [])

withDb :: Example (IO b) => String -> AWS b -> SpecWith (Arg (IO b))
withDb msg code = it msg runcode
  where
    runcode = do
      setEnv "AWS_ACCESS_KEY_ID" "XXXXXXXXXXXXXX"
      setEnv "AWS_SECRET_ACCESS_KEY" "XXXXXXXXXXXXXXfdjdsfjdsfjdskldfs+kl"
      lgr  <- newLogger Debug stdout
      env  <- newEnv Discover
      let dynamo = setEndpoint False "localhost" 8000 dynamoDB
      let newenv = env & configure dynamo
                       -- & set envLogger lgr
      runResourceT $ runAWS newenv $ do
          deleteTable (Proxy :: Proxy Test) `catchAny` (\_ -> return ())
          migrateTest mempty Nothing
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
    withDb "scanCond works correctly" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- scanCond (colIInt >. 50) 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "scan works correctly with qlimit" $ do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch newItems
      (items, _) <- scan (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (colIInt >. 50)) 5
      liftIO $ items `shouldBe` drop 50 newItems
    withDb "scan works correctly with `valIn`" $ do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch newItems
      (items, _) <- scan (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (colIInt `valIn` [20..30])) 50
      liftIO $ map iInt items `shouldBe` [20..30]
    withDb "scan works correctly with BETWEEN" $ do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch newItems
      (items, _) <- scan (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (colIInt `between` (20, 30))) 50
      liftIO $ map iInt items `shouldBe` [20..30]
    withDb "scan works correctly with SIZE" $ do
      let testitem1 = Test "1" 2 "very very very very very long" False 3.14 2 Nothing
          testitem2 = Test "1" 3 "short" False 3.14 2 Nothing
      putItem testitem1
      putItem testitem2
      (items, _) <- scan (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (size colIText >. 10)) 50
      liftIO $ items `shouldBe` [testitem1]
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

    withDb "update fails on non-existing item" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 2 (Just "something")
        putItem testitem1
        updateItemByKey_ (Proxy :: Proxy Test, ("1", 2)) (colIBool =. True)
        (res :: Either SomeException ()) <- try $ updateItemByKey_ (Proxy :: Proxy Test, ("2", 3)) (colIBool =. True)
        liftIO $ res `shouldSatisfy` isLeft

    withDb "scan continuation works" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems

        (it1 :: [Test], next) <- scan (scanOpts & sFilterCondition .~ Just (colIInt >. 20)
                                                & sLimit .~ Just 2) 5
        (it2, _) <- scan (scanOpts & sFilterCondition .~ Just (colIInt >. 20)
                                                & sLimit .~ Just 1
                                                & sStartKey .~ next) 5
        liftIO $ map iInt (it1 ++ it2) `shouldBe` [21..30]

    withDb "searching empty strings" $ do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem testitem1
        putItem testitem2
        (items1 :: [Test]) <- queryCond "1" Nothing (colIText ==. "") Forward 10
        (items2 :: [Test]) <- queryCond "1" Nothing (colIMText ==. Nothing) Forward 10
        liftIO $ items1 `shouldBe` [testitem1]
        liftIO $ items2 `shouldBe` [testitem1]

    withDb "deleting by key" $ do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem testitem1
        putItem testitem2
        (items :: [Test], _) <- scan scanOpts 10
        deleteItemByKey (itemToKey testitem1)
        deleteItemByKey (itemToKey testitem2)
        (items2 :: [Test], _) <- scan scanOpts 10
        liftIO $ do
          length items `shouldBe` 2
          length items2 `shouldBe` 0


main :: IO ()
main = hspec spec
