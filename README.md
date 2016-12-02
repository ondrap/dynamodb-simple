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
      migrate  (provisionedThroughput 5 5) [] -- Create tables, indices etc.
      --
      putItemBatch ((Test "news" "john" "test" 20) :| [])
      --
      item <- getItem Eventually ("news", "john")
      liftIO $ print (item :: Maybe Test)
      --
      runConduit $
          scanCond Eventually (colReplies >. 15)
            =$= CL.mapM_ (\(i :: Test) -> liftIO (print i))
````

### Limitations

- Local secondary index are not supported. It could be probably supported similarly to the global secondary indexes.
- Projections are not supported. Using some generic programming on tuples it should be possible.
- Translation of field names to attribute names is hardcoded. It might be possible to parametrize it by
  parametrizing the DynamoCollection or DynamoTable class.
- Table name is hardcoded; this would be easy to solve in TH.
- You cannot compare attributes between themselves (i.e. `colCurrentAccount >=. colAverageAccount`).
  This would be possible with a different set of operators, it might be possible with overloading the current operators. Does anybody need it?

### Handling of NULLs

DynamoDB does not accept empty strings/sets. On the other hand
there is a difference between `NULL` and `missing field`.
It's not obvious how to represent the `Maybe a` datatypes.

In this framework an empty string/set is represented as `NULL`, `Nothing` is represented by omitting the value.
Empty set is represented by omitting the value. This way you can still relatively safely
use `Maybe Text`.

* `Just Nothing :: Maybe (Maybe a)` will become `Nothing` on retrieval.
* `[Maybe a]` is not a good idea. `[Just 1, Nothing, Just 3]` will become `[Just 1, Just 3]` on retrieval.
* `HashMap Text (Maybe a)` is not a good idea either; missing values will disappear.
* `Maybe (Set a)` will become `Nothing` on empty set
* Don't try to use inequality comparisons (`>.`, `<.`) on empty strings.
* If you use `colMaybeCol == Nothing`, it gets internally replaced
  by `attr_missing(colMaybeCol)`, so it will behave as expected.
* In case of schema change, `Maybe` columns are considered `Nothing`.
* In case of schema change, `String` columns are decoded as empty strings, `Set` columns
  as empty sets.
* Condition for `== ""`, `== []` etc. is automatically enhanced to account for non-existent attributes
  (i.e. after schema change).
* Empty list/empty hashmap is represented as empty list/hashmap; however it is allowed to be decoded
  even when the attribute is missing in order to allow better schema migrations.
