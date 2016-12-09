# DynamoDB layer for Haskell

[![Build Status](https://travis-ci.org/ondrap/dynamodb-simple.svg?branch=master)](https://travis-ci.org/ondrap/dynamodb-simple) [![Hackage](https://img.shields.io/hackage/v/dynamodb-simple.svg)](https://hackage.haskell.org/package/dynamodb-simple)

This library intends to simplify working with DynamoDB AWS database.
It uses Generics code ([generics-sop](https://hackage.haskell.org/package/generics-sop)) on top of your structures
and just by adding a few instances allows you to easily generate AWS
commands.

````haskell
data Test = Test {
    category :: T.Text
  , user     :: T.Text
  , subject  :: T.Text
  , replies  :: Int
} deriving (Show)
-- Generate instances and category', user' etc. variables for queries/updates
mkTableDefs "migrate" (tableConfig (''Test, WithRange) [] [])

test :: IO ()
test = do
  lgr  <- newLogger Info stdout
  setEnv "AWS_ACCESS_KEY_ID" "XXXXXXXXXXXXXX"
  setEnv "AWS_SECRET_ACCESS_KEY" "XXXXXXXXXXXXXXfdjdsfjdsfjdskldfs+kl"
  env  <- newEnv Discover
  let dynamo = setEndpoint False "localhost" 8000 dynamoDB
  let newenv = env & configure dynamo & set envLogger lgr
  runResourceT $ runAWS newenv $ do
      migrate mempty Nothing -- Create tables, indices etc.
      --
      putItem (Test "news" "john" "test" 20)
      --
      item <- getItem Eventually (tTest, ("news", "john"))
      liftIO $ print item
      --
      items <- scanCond tTest (replies' >. 15) 10
      liftIO $ print items
````
### Features

- Global secondary indexes.
- Local secondary indexes.
- Tables with only hash keys as well as tables with combined hash and sort key.
- Sparse indexes (define the column as `Maybe` in a table, omit the `Maybe` in index definition).
- Automatically generate polymorphic lenses to access fields in main table and index records.
- Standard datatypes including `Tagged` and basic default instances for data types supporting
  `Show/Read`.
- New types can be added easily.
- High-level, easy-to-use API - hides intricacies of both DynamoDB and amazonka library.
- Type-safe conditions, including nested structures.
- Type-safe update actions.
- Template-haskell macro to easily create all relevant instances.
- 'Schema migration' - upon startup checks if the database schema matches the definition
  and, if possible, adjusts the database. If it is impossible, it fails.
- Automatic handling of invalid values (empty strings, empty sets). Automatic rewriting of
  queries when searching for these empty values.
- Compatible with GHC8 `DuplicateRecordFields`
- Customizable table and index names. Custom translation of field names to attribute names.
- Streaming settings.

### What is planned

- Support for automatic versioning of fields.

### Limitations

- Projections are not supported. Using some generic programming on tuples it should be possible.
- You cannot compare attributes between themselves (i.e. `currentAccount' >=. averageAccount'`).
  I'm not sure this would be currently technically possible. Does anybody need it?

### Handling of NULLs

DynamoDB does not accept empty strings/sets. It accepts `NULL`, but that is not acceptable
in fields that are used for sparse indexing.

Empty string and empty set are represented by omitting the value.

* `Just Nothing :: Maybe (Maybe a)` will become `Nothing` on retrieval.
* `[Just 1, Nothing, Just 3]` will become `[Just 1, Just 3]` on retrieval.
* `HashMap Text (Maybe a)` is not a good idea; missing values will disappear.
* `Maybe (Set a)` will become `Nothing` on empty set
* Don't try to use inequality comparisons (`>.`, `<.`) on empty strings.
* If you use `maybeCol' == Nothing`, it gets internally replaced
  by `attr_missing(maybeCol)`, so it will behave as expected. The same with
  empty `String` or `Set`. Keep that in mind when traversing nested structures.
* In case of schema change, `Maybe` columns are considered `Nothing`.
* In case of schema change, `String` columns are decoded as empty strings, `Set` columns
  as empty sets, `[a]` columns as empty lists.
* Condition for `== ""`, `== []` etc. is automatically enhanced to account for non-existent attributes
  (i.e. after schema change).
* Empty list/empty hashmap is represented as empty list/hashmap; however it is allowed to be decoded
  even when the attribute is missing in order to allow better schema migrations.
