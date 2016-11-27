## DynamoDB layer for Haskell

This library intends to simplify working with DynamoDB AWS database.
It uses Generics code (using generics-sop) on top of your structures
and just by adding a few instances allows you to easily generate AWS
commands.

````haskell

import Generics.SOP
import qualified GHC.Generics                     as GHC

import Database.DyamoDB

data Test = Test {
    category :: T.Text
  , user     :: T.Text
  , subject  :: T.Text
  , replies  :: Int
} deriving (Show, GHC.Generic)
instance Generic Test -- Derive the generic-sop instance
instance HasDatatypeInfo Test -- Derive generic-sop metadata

-- Choose one of thesee instances - creates table Test
-- For tables with only a hashkey; first record field is a hashkey
instance DynamoTable Test NoRange
-- For tables with hashkey and sort key; first 2 fields form primary key;
-- override default table name
instance DynamoTable Test WithRange
  tableName = "myTableName"
--

-- Generate AWS CreateTable for this table
createTable (Proxy :: Proxy Test)
-- Generate AWS PutItem to the table
putItem (Test "category" "john doe" "Intersting subject", 3)
-- Generate a Query just with a hash key
queryKey (Proxy :: Proxy Test) "category"
-- Generate a Query with hash and range key comparison; if a table doesn't have
-- a range key, it has the same syntax as queryKey
queryKeyRange (Proxy :: Proxy Test) ("category", Equal "john doe")
````
