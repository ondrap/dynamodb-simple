module Database.DynamoDb (
    DynamoCollection(..)
  , DynamoTable(..)
  , DynamoIndex(..)
  , queryKey, queryKeyRange
  , NoRange, WithRange
  , IsTable, IsIndex
) where

import Database.DynamoDb.Class
