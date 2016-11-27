## DynamoDB layer for Haskell

This library intends to simplify working with DynamoDB AWS database.
It uses Generics code (using generics-sop) on top of your structures
and just by adding a few instances allows you to easily generate AWS
commands.

````haskell

import           Generics.SOP
import qualified GHC.Generics                     as GHC

import Database.DyamoDB

data Test = Test {
    prvni :: Int
  , druhy :: T.Text
} deriving (Show, GHC.Generic)
instance Generic Test
instance HasDatatypeInfo Test
instance DynamoTable Test NoRange where

````
