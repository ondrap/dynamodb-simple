{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE EmptyDataDecls        #-}
{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE TemplateHaskell       #-}
{-# LANGUAGE TypeSynonymInstances  #-}
{-# LANGUAGE TypeFamilies          #-}

module NestedSpec where

import           Control.Exception.Safe   (catchAny, finally)
import           Control.Monad.IO.Class   (liftIO)
import           Control.Monad.Trans.Resource (ResourceT)
import           Control.Lens             (makeLenses, (^.), (^?), ix, (^..))
import           Data.Function            ((&))
import           Data.Hashable
import           Data.HashMap.Strict      (HashMap)
import qualified Data.HashMap.Strict      as HMap
import           Data.Maybe
import           Data.Proxy
import qualified Data.Set                 as Set
import           Data.Tagged
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
import           Database.DynamoDB.Update

data TName
type Name = Tagged TName T.Text
instance Hashable Name

data Inner = Inner {
    _nFirst  :: T.Text
  , _nSecond :: Maybe Int
  , _nThird :: T.Text
} deriving (Show, Eq)
deriveCollection ''Inner defaultTranslate
makeLenses ''Inner

data Test = Test {
    _iHashKey  :: T.Text
  , _iRangeKey :: Int
  , _iInner    :: Inner
  , _iMInner   :: Maybe Inner
  , _iSet      :: Set.Set Name
  , _iList     :: [Inner]
  , _iMap      :: HashMap Name Inner
} deriving (Show, Eq)
mkTableDefs "migrateTest" (tableConfig "" (''Test, WithRange) [] [])

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
          code newenv `finally` deleteTable newenv (Proxy :: Proxy Test)

spec :: Spec
spec = do
  describe "Nested structures" $ do
    withDb "Save and retrieve" $ \env -> do
        let inner1 = Inner "test" (Just 3) "test"
            inner2 = Inner "" Nothing ""
            testitem1 = Test "hash" 1 inner1 (Just inner2) (Set.singleton (Tagged "test")) [inner1] (HMap.singleton "test" inner2)
            testitem2 = Test "hash" 2 inner1 Nothing Set.empty [] HMap.empty
        putItem env testitem1
        putItem env testitem2
        ritem1 <- fromJust <$> getItem env Strongly tTest ("hash", 1)
        ritem2 <- fromJust <$> getItem env Strongly tTest ("hash", 2)
        liftIO $ testitem1 `shouldBe` ritem1
        liftIO $ testitem2 `shouldBe` ritem2
    withDb "Scan conditions" $ \env -> do
        let inner1 = Inner "test" (Just 3) ""
            inner2 = Inner "" Nothing "test"
            testitem1 = Test "hash" 1 inner1 (Just inner2) (Set.singleton (Tagged "test")) [inner1] (HMap.singleton "test" inner2)
            testitem2 = Test "hash" 2 inner2 Nothing Set.empty [] HMap.empty
        putItem env testitem1
        putItem env testitem2
        --
        items1 <- scanCond env tTest (iInner' <.> nFirst' ==. "test") 10
        liftIO $ items1 `shouldBe` [testitem1]
        --
        items2 <- scanCond env tTest (iInner' <.> nFirst' ==. "") 10
        liftIO $ items2 `shouldBe` [testitem2]
        --
        items3 <- scanCond env tTest (iMInner' <.> nThird' ==. "test") 10
        liftIO $ items3 `shouldBe` [testitem1]
        --
        items4 <- scanCond env tTest (iMInner' ==. Nothing) 10
        liftIO $ items4 `shouldBe` [testitem2]
        --
        items5 <- scanCond env tTest (iSet' `setContains` Tagged "test") 10
        liftIO $ items5 `shouldBe` [testitem1]
        --
        items6 <- scanCond env tTest (iList' <!> 0 <.> nFirst' ==. "test") 10
        liftIO $ items6 `shouldBe` [testitem1]
        --
        items7 <- scanCond env tTest (iMap' <!:> Tagged "test" <.> nThird' ==. "test") 10
        liftIO $ items7 `shouldBe` [testitem1]

    withDb "Nested updates" $ \env -> do
        let inner1 = Inner "test" (Just 3) ""
            inner2 = Inner "" Nothing "test"
            testitem1 = Test "hash" 1 inner1 (Just inner2) (Set.singleton (Tagged "test")) [inner1] (HMap.singleton "test" inner2)
            testitem2 = Test "hash" 2 inner2 Nothing Set.empty [] HMap.empty
        putItem env testitem1
        putItem env testitem2
        --
        newitem1 <- updateItemByKey env tTest (tableKey testitem1) (iInner' <.> nFirst' =. "updated")
        liftIO $ newitem1 ^. iInner . nFirst `shouldBe` "updated"
        --
        newitem2 <- updateItemByKey env tTest (tableKey testitem1) (add iSet' (Set.singleton (Tagged "test2")))
        liftIO $ newitem2 ^. iSet `shouldBe` Set.fromList [Tagged "test", Tagged "test2"]
        --
        newitem3 <- updateItemByKey env tTest (tableKey testitem1) (prepend iList' [inner2, inner1])
        liftIO $ newitem3 ^. iList `shouldBe` [inner2, inner1, inner1]
        --
        newitem4 <- updateItemByKey env tTest (tableKey testitem1) (delListItem iList' 1)
        liftIO $ newitem4 ^. iList `shouldBe` [inner2, inner1]
        --
        newitem5 <- updateItemByKey env tTest (tableKey testitem2) (iMap' <!:> Tagged "test" =. inner1)
        liftIO $ newitem5 ^.. iMap . traverse `shouldBe` [inner1]
        liftIO $ newitem5 ^? iMap . ix (Tagged "test")  `shouldBe` Just inner1
        --
        newitem6 <- updateItemByKey env tTest (tableKey testitem2) (delHashKey iMap' (Tagged "test"))
        liftIO $ newitem6 ^. iMap `shouldBe` HMap.empty


main :: IO ()
main = hspec spec
