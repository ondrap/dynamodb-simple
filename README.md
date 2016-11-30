## DynamoDB layer for Haskell

This library intends to simplify working with DynamoDB AWS database.
It uses Generics code (using generics-sop) on top of your structures
and just by adding a few instances allows you to easily generate AWS
commands.

````haskell

import Generics.SOP
import qualified GHC.Generics                     as GHC

import Database.DyamoDB
import Database.DyamoDB.TH

data Test = Test {
    category :: T.Text
  , user     :: T.Text
  , subject  :: T.Text
  , replies  :: Int
} deriving (Show, GHC.Generic)

data TestIndex = TestIndex {
    i_subject :: T.Text
  , i_user :: T.Text
  , i_replies :: Int
} deriving (Show, GHC.Generic)
-- Generate migrate fucntion, instances and column constructors
$(mkTableDefs "migrate" (''Test, True) [(''TestIndex, True)])

test :: IO ()
test = do
  lgr  <- newLogger Info stdout
  env  <- newEnv NorthVirginia Discover
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  let newenv = env & configure dynamo
                   & set envLogger lgr
  runResourceT $ runAWS newenv $ do
      -- Create tables, indices etc.
      migrate
      --
      liftIO $ print "Putting item"
      putItemBatch ((Test "news" "john" "test" 0) :| [])
      --
      liftIO $ print "Getting item"
      item <- getItem Eventually ("news", "john")
      liftIO $ print (item :: Maybe Test)
      --
      liftIO $ print "Deleting item"
      -- Choose one way to delete an item
      deleteItem (Proxy :: Proxy Test) ("news", "john")
      deleteItemCond (Proxy :: Proxy Test) ("news", "john") (colSubject ==. "test")
      deleteItemBatch (Proxy :: Proxy Test) (("news", "john") :| [])
      --
      liftIO $ print "Getting item"
      item2 <- getItem Eventually ("news", "john")
      liftIO $ print (item2 :: Maybe Test)
````

### Handling of NULLs

DynamoDB does not accept empty strings. On the other hand there is a difference between `NULL` and `missing field`.
It's not obvious how to represent the `Maybe a` datatype and in particular `Maybe Text` datatype.

In this framework an empty string is represented as `NULL`, `Nothing` is represented by omitting the value.

* `Just Nothing :: Maybe (Maybe a)` will become `Nothing` on retrieval
* `[Maybe a]` is not a good idea. `[Just 1, Nothing, Just 3]` will become `[Just 1, Just 3]` on retrieval
* `HashMap Text (Maybe a)` is not a good idea either; missing values will disappear
* Don't try to use inequality comparisons (`>.`, `<.`) on empty strings
* If you use `colMaybeCol == Nothing`, it gets internally replaced
  by `attr_missing(colMaybeCol)`.
