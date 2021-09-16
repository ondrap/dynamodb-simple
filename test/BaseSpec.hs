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
import           Control.Monad.Trans.Resource (ResourceT(..))
import           Data.Conduit             (runConduit, (=$=))
import qualified Data.Conduit.List        as CL
import           Data.Either              (isLeft)
import           Data.Function            ((&))
import           Data.List                (sort)
import           Data.Proxy
import           Data.Semigroup           ((<>))
import qualified Data.Text                as T
import           Network.AWS
import qualified Network.AWS.DynamoDB
import           System.Environment       (setEnv)
import           System.IO                (stdout)
import           Test.Hspec
import           Data.Maybe               (fromJust)

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

data TestKeyOnly = TestKeyOnly {
    d_iText :: T.Text
  , d_iHashKey  :: T.Text
}
mkTableDefs "migrateTest" (tableConfig "" (''Test, WithRange) [(''TestKeyOnly, NoRange)] [])

data TestSecond = TestSecond {
    tHashKey :: T.Text
  , tInt :: Int
} deriving (Show, Eq)
mkTableDefs "migrateTest2" (tableConfig "" (''TestSecond, NoRange) [] [])

withDb :: Example (IO b) => String -> (Env -> ResourceT IO b) -> SpecWith (Arg (IO b))
withDb msg code = it msg runcode
  where
    runcode = do
      setEnv "AWS_ACCESS_KEY_ID" "XXXXXXXXXXXXXX"
      setEnv "AWS_SECRET_ACCESS_KEY" "XXXXXXXXXXXXXXfdjdsfjdsfjdskldfs+kl"
      lgr  <- newLogger Debug stdout
      env  <- newEnv Discover
      let dynamo = setEndpoint False "localhost" 8000 Network.AWS.DynamoDB.defaultService
      let newenv = env & configure dynamo
                       -- & set envLogger lgr
      runResourceT $ do
          deleteTable newenv (Proxy :: Proxy Test) `catchAny` (\_ -> return ())
          migrateTest newenv mempty Nothing
          migrateTest2 newenv mempty Nothing
          code newenv `finally` deleteTable newenv (Proxy :: Proxy Test)

spec :: Spec
spec = do
  describe "Basic operations" $ do
    withDb "putItem/getItem works" $ \env -> do
        let testitem1 = Test "1" 2 "text" False 3.14 2 Nothing
            testitem2 = Test "2" 3 "text" False 4.15 3 (Just "text")
        putItem env testitem1
        putItem env testitem2
        it1 <- getItem env Strongly tTest ("1", 2)
        it2 <- getItem env Strongly tTest ("2", 3)
        liftIO $ Just testitem1 `shouldBe` it1
        liftIO $ Just testitem2 `shouldBe` it2
    withDb "getItemBatch/putItemBatch work" $ \env -> do
        let template i = Test (T.pack $ show i) i "text" False 3.14 i Nothing
            newItems = map template [1..300]
        putItemBatch env newItems
        --
        let keys = map tableKey newItems
        items <- getItemBatch env Strongly keys
        liftIO $ sort items `shouldBe` sort newItems
    withDb "insertItem doesn't overwrite items" $ \env -> do
        let testitem1 = Test "1" 2 "text" False 3.14 2 Nothing
            testitem1_ = Test "1" 2 "XXXX" True 3.14 3 Nothing
        insertItem env testitem1
        (res :: Either SomeException ()) <- try (insertItem env testitem1_)
        liftIO $ res `shouldSatisfy` isLeft
    withDb "scanSource works correctly with sLimit" $ \env -> do
        let template i = Test (T.pack $ show i) i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems
        let squery = scanOpts & sFilterCondition .~ Just (iInt' >. 50)
                              & sLimit .~ Just 1
        res <- runConduit $ scanSource env tTest squery =$= CL.consume
        liftIO $ sort res `shouldBe` sort (drop 50 newItems)
    withDb "querySource works correctly with qLimit" $ \env -> do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems
        let squery = queryOpts "hashkey" & qFilterCondition .~ Just (iInt' >. 50)
                                         & qLimit .~ Just 1
        res <- runConduit $ querySource env tTest squery =$= CL.consume
        liftIO $ res `shouldBe` drop 50 newItems
    withDb "queryCond works correctly with qLimit" $ \env -> do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems
        items <- queryCond env tTest "hashkey" Nothing (iInt' >. 50) Forward 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "scanCond works correctly" $ \env -> do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems
        items <- scanCond env tTest (iInt' >. 50) 5
        liftIO $ items `shouldBe` drop 50 newItems
    withDb "scan works correctly with qlimit" $ \env -> do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch env newItems
      (items, _) <- scan env tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (iInt' >. 50)) 5
      liftIO $ items `shouldBe` drop 50 newItems
    withDb "scan works correctly with `valIn`" $ \env -> do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch env newItems
      (items, _) <- scan env tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (iInt' `valIn` [20..30])) 50
      liftIO $ map iInt items `shouldBe` [20..30]
    withDb "scan works correctly with BETWEEN" $ \env -> do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch env newItems
      (items, _) <- scan env tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (iInt' `between` (20, 30))) 50
      liftIO $ map iInt items `shouldBe` [20..30]
    withDb "scan works correctly with SIZE" $ \env -> do
      let testitem1 = Test "1" 2 "very very very very very long" False 3.14 2 Nothing
          testitem2 = Test "1" 3 "short" False 3.14 2 Nothing
      putItem env testitem1
      putItem env testitem2
      (items, _) <- scan env tTest (scanOpts & sLimit .~ Just 1 & sFilterCondition .~ Just (size iText' >. 10)) 50
      liftIO $ items `shouldBe` [testitem1]
    withDb "querySimple works correctly with RangeOper" $ \env -> do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems
        items <- querySimple env tTest "hashkey" (Just $ RangeLessThan 30) Backward 5
        liftIO $ map iRangeKey items `shouldBe` [29,28..25]
    withDb "queryCond works correctly with -1 limit" $ \env -> do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems
        (items :: [Test]) <- queryCond env tTest "hashkey" Nothing (iInt' >. 50) Backward (-1)
        liftIO $ items `shouldBe` []
    withDb "querySourceByKey works/compiles correctly" $ \env -> do
      let template i = Test "hashkey" i "text" False 3.14 i Nothing
          newItems = map template [1..55]
      putItemBatch env newItems
      res <- runConduit $ querySourceByKey env iTestKeyOnly "text" =$= CL.consume
      liftIO $ length res `shouldBe` 55

    withDb "updateItemByKey works" $ \env -> do
        let testitem1 = Test "1" 2 "text" False 3.14 2 (Just "something")
        putItem env testitem1
        new1 <- updateItemByKey env tTest (tableKey testitem1)
                                    ((iInt' +=. 5) <> (iText' =. "updated") <> (iMText' =. Nothing))
        new2 <- fromJust <$> getItem env Strongly tTest (tableKey testitem1)
        liftIO $ do
            new1 `shouldBe` new2
            iInt new1 `shouldBe` 7
            iText new1 `shouldBe` "updated"
            iMText new1 `shouldBe` Nothing

    withDb "update fails on non-existing item" $ \env -> do
        let testitem1 = Test "1" 2 "text" False 3.14 2 (Just "something")
        putItem env testitem1
        updateItemByKey_ env tTest ("1", 2) (iBool' =. True)
        (res :: Either SomeException ()) <- try $ updateItemByKey_ env tTest ("2", 3) (iBool' =. True)
        liftIO $ res `shouldSatisfy` isLeft

    withDb "scan continuation works" $ \env -> do
        let template i = Test "hashkey" i "text" False 3.14 i Nothing
            newItems = map template [1..55]
        putItemBatch env newItems

        (it1, next) <- scan env tTest (scanOpts & sFilterCondition .~ Just (iInt' >. 20)
                                            & sLimit .~ Just 2) 5
        (it2, _) <- scan env tTest (scanOpts & sFilterCondition .~ Just (iInt' >. 20)
                                         & sLimit .~ Just 1
                                         & sStartKey .~ next) 5
        liftIO $ map iInt (it1 ++ it2) `shouldBe` [21..30]

    withDb "searching empty strings" $ \env -> do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem env testitem1
        putItem env testitem2
        items1 <- queryCond env tTest "1" Nothing (iText' ==. "") Forward 10
        items2 <- queryCond env tTest "1" Nothing (iMText' ==. Nothing) Forward 10
        liftIO $ items1 `shouldBe` [testitem1]
        liftIO $ items2 `shouldBe` [testitem1]

    withDb "deleting by key" $ \env -> do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "test")
        putItem env testitem1
        putItem env testitem2
        (items, _) <- scan env tTest scanOpts 10
        deleteItemByKey env tTest (tableKey testitem1)
        deleteItemByKey env tTest (tableKey testitem2)
        (items2, _) <- scan env tTest scanOpts 10
        liftIO $ do
          length items `shouldBe` 2
          length items2 `shouldBe` 0

    withDb "test left join" $ \env -> do
        let testitem1 = Test "1" 2 "" False 3.14 2 Nothing
        let testitem2 = Test "1" 3 "aaa" False 3.14 2 (Just "aaa")
        let testsecond = TestSecond "aaa" 3
        putItem env testitem1
        putItem env testitem2
        putItem env testsecond
        res <- runConduit $ querySourceChunks env tTest (queryOpts "1")
                              =$= leftJoin env Strongly tTestSecond (Just . iText)
                              =$= CL.concat =$= CL.consume
        liftIO $ res `shouldBe` [(testitem1, Nothing), (testitem2, Just testsecond)]
        res2 <- runConduit $ querySourceChunks env tTest (queryOpts "1")
                              =$= leftJoin env Strongly tTestSecond iMText
                              =$= CL.concat =$= CL.consume
        liftIO $ res2 `shouldBe` [(testitem1, Nothing), (testitem2, Just testsecond)]

main :: IO ()
main = hspec spec
