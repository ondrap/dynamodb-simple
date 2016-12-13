{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE FlexibleInstances     #-}

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
        it1 <- getItem Strongly (tTest, ("1", 2))
        it2 <- getItem Strongly (tTest, ("2", 3))
        liftIO $ Just testitem1 `shouldBe` it1
        liftIO $ Just testitem2 `shouldBe` it2
    withDb "getItemBatch/putItemBatch work" $ do
        let template i = Test (T.pack $ show i) i "text" False 3.14 i Nothing
            newItems = map template [1..300]
        putItemBatch newItems
        --
        let keys = map (snd . tableKey) newItems
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
        let squery = scanOpts & sFilterCondition .~ Just (iInt' >. 50)
                              & sLimit .~ Just 1
        res <- runConduit $ scanSource tTest squery =$= CL.consume
        liftIO $ sort res `shouldBe` sort (drop 50 newItems)
    withDb "querySource works correctly with qLimit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        let squery = queryOpts "hashkey" & qFilterCondition .~ Just (iInt' >. 50)
                                         & qLimit .~ Just 1
        res <- runConduit $ querySource tTest squery =$= CL.consume
        liftIO $ res `shouldBe` drop 50 newItems
    withDb "queryCond works correctly with qLimit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- queryCond tTest "hashkey" Nothing (iInt' >. 50) Forward 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "scanCond works correctly" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- scanCond tTest (iInt' >. 50) 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "scan works correctly with qlimit" $ do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch newItems
      (items, _) <- scan tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (iInt' >. 50)) 5
      liftIO $ items `shouldBe` drop 50 newItems
    withDb "scan works correctly with `valIn`" $ do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch newItems
      (items, _) <- scan tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (iInt' `valIn` [20..30])) 50
      liftIO $ map iInt items `shouldBe` [20..30]
    withDb "scan works correctly with BETWEEN" $ do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch newItems
      (items, _) <- scan tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (iInt' `between` (20, 30))) 50
      liftIO $ map iInt items `shouldBe` [20..30]
    withDb "scan works correctly with SIZE" $ do
      let testitem1 = Test "1" 2 "very very very very very long" False 3.14 2 Nothing
          testitem2 = Test "1" 3 "short" False 3.14 2 Nothing
      putItem testitem1
      putItem testitem2
      (items, _) <- scan tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (size iText' >. 10)) 50
      liftIO $ items `shouldBe` [testitem1]
    withDb "querySimple works correctly with RangeOper" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        items <- querySimple tTest "hashkey" (Just $ RangeLessThan 30) Backward 5
        liftIO $ map iRangeKey items `shouldBe` [29,28..25]
    withDb "queryCond works correctly with -1 limit" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems
        (items :: [Test]) <- queryCond tTest "hashkey" Nothing (iInt' >. 50) Backward (-1)
        liftIO $ items `shouldBe` []
    withDb "updateItemByKey works" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 2 (Just "something")
        putItem testitem1
        new1 <- updateItemByKey (tableKey testitem1)
                                ((iInt' +=. 5) <> (iText' =. "updated") <> (iMText' =. Nothing))
        Just new2 <- getItem Strongly (tableKey testitem1)
        liftIO $ do
            new1 `shouldBe` new2
            iInt new1 `shouldBe` 7
            iText new1 `shouldBe` "updated"
            iMText new1 `shouldBe` Nothing

    withDb "update fails on non-existing item" $ do
        let testitem1 = Test "1" 2 "text" False 3.14 2 (Just "something")
        putItem testitem1
        updateItemByKey_ (tTest, ("1", 2)) (iBool' =. True)
        (res :: Either SomeException ()) <- try $ updateItemByKey_ (tTest, ("2", 3)) (iBool' =. True)
        liftIO $ res `shouldSatisfy` isLeft

    withDb "scan continuation works" $ do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch newItems

        (it1, next) <- scan tTest (scanOpts & sFilterCondition .~ Just (iInt' >. 20)
                                            & sLimit .~ Just 2) 5
        (it2, _) <- scan tTest (scanOpts & sFilterCondition .~ Just (iInt' >. 20)
                                         & sLimit .~ Just 1
                                         & sStartKey .~ next) 5
        liftIO $ map iInt (it1 ++ it2) `shouldBe` [21..30]

    withDb "searching empty strings" $ do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem testitem1
        putItem testitem2
        items1 <- queryCond tTest "1" Nothing (iText' ==. "") Forward 10
        items2 <- queryCond tTest "1" Nothing (iMText' ==. Nothing) Forward 10
        liftIO $ items1 `shouldBe` [testitem1]
        liftIO $ items2 `shouldBe` [testitem1]

    withDb "deleting by key" $ do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem testitem1
        putItem testitem2
        (items, _) <- scan tTest scanOpts 10
        deleteItemByKey (tableKey testitem1)
        deleteItemByKey (tableKey testitem2)
        (items2, _) <- scan tTest scanOpts 10
        liftIO $ do
          length items `shouldBe` 2
          length items2 `shouldBe` 0

    withDb "test left join" $ do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem testitem1
        putItem testitem2
        res <- leftJoin Strongly tTest [("first", ("1",2)), ("missing", ("1",5))]
        liftIO $ res `shouldBe` [("first" :: String, Just testitem1), ("missing", Nothing)]


main :: IO ()
main = hspec spec
