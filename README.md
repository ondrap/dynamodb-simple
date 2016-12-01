# DynamoDB layer for Haskell

[![Build Status](https://travis-ci.org/ondrap/dynamodb-simple.svg?branch=master)](https://travis-ci.org/ondrap/dynamodb-simple) [![Hackage](https://img.shields.io/hackage/v/dynamodb-simple.svg)](https://hackage.haskell.org/package/dynamodb-simple)

This library intends to simplify working with DynamoDB AWS database.
It uses Generics code (using generics-sop) on top of your structures
and just by adding a few instances allows you to easily generate AWS
commands.

````haskell
data Test = Test {
    category :: T.Text
  , user     :: T.Text
  , subject  :: T.Text
  , replies  :: Int
} deriving (Show, GHC.Generic)
$(mkTableDefs "migrate" (''Test, True) [])

test :: IO ()
test = do
  lgr  <- newLogger Info stdout
  env  <- newEnv NorthVirginia Discover
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  let newenv = env & configure dynamo & set envLogger lgr
  runResourceT $ runAWS newenv $ do
      migrate -- Create tables, indices etc.
      --
      putItemBatch ((Test "news" "john" "test" 0) :| [])
      --
      item <- getItem Eventually ("news", "john")
      liftIO $ print (item :: Maybe Test)
````

### Limitations



### Handling of NULLs

DynamoDB does not accept empty strings/sets. On the other hand
there is a difference between `NULL` and `missing field`.
It's not obvious how to represent the `Maybe a` datatype and in particular `Maybe Text` datatype.

In this framework an empty string/set is represented as `NULL`, `Nothing` is represented by omitting the value.

* `Just Nothing :: Maybe (Maybe a)` will become `Nothing` on retrieval
* `[Maybe a]` is not a good idea. `[Just 1, Nothing, Just 3]` will become `[Just 1, Just 3]` on retrieval
* `HashMap Text (Maybe a)` is not a good idea either; missing values will disappear
* Don't try to use inequality comparisons (`>.`, `<.`) on empty strings
* If you use `colMaybeCol == Nothing`, it gets internally replaced
  by `attr_missing(colMaybeCol)`, so it will behave as expected.
* In case of schema change, `Maybe` columns are considered `Nothing`
* In case of schema change, `String` columns are decoded as empty strings, `Set` columns
  as empty sets
* Filtres for `== ""` (and empty sets) are automatically enhanced to account for
  non-existent attributes (i.e. after schema change)
