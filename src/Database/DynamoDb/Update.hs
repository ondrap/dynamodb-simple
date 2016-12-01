{-# LANGUAGE DataKinds        #-}
{-# LANGUAGE FlexibleContexts #-}

module Database.DynamoDb.Update (
    Action
  , (+=.), (-=.), (=.)
  , setIfNotExists
  , append, prepend
  , add, delete
) where

import           Network.AWS.DynamoDB.Types (AttributeValue)
import qualified Data.Set as Set

import           Database.DynamoDb.Internal
import           Database.DynamoDb.Types

data ActionValue t =
    ValAttr AttributeValue
    | IfNotExists NameGen AttributeValue
    | ListAppend NameGen AttributeValue
    | ListPrepend NameGen AttributeValue

data Action' t =
    Plus NameGen AttributeValue -- Add number to existing value
    | Minus NameGen AttributeValue -- Subtract number from existing value
    | Set NameGen (ActionValue t)  -- General SET
    | Add NameGen AttributeValue -- Add value to a Set
    | Delete NameGen AttributeValue -- Delete value from a Set
    | Remove NameGen -- Remove attribute



dumpAction :: Action' t -> ()
dumpAction act = undefined

-- | Type representing an action for updateItem
type Action t = Maybe (Action' t)

(+=.) :: (InCollection col tbl 'FullPath, DynamoScalar typ, IsNumber typ)
    => Column typ ctyp col -> typ -> Action tbl
(+=.) col val = Just $ Plus (nameGen col) (dScalarEncode val)

(-=.) :: (InCollection col tbl 'FullPath, DynamoScalar typ, IsNumber typ)
    => Column typ ctyp col -> typ -> Action tbl
(-=.) col val = Just $ Minus (nameGen col) (dScalarEncode val)

(=.) ::  (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ ctyp col -> typ -> Action tbl
(=.) col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (ValAttr attr)
    Nothing -> Just $ Remove (nameGen col)

setIfNotExists :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ ctyp col -> typ -> Action tbl
setIfNotExists col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (IfNotExists (nameGen col) attr)
    Nothing -> Nothing

append :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] ctyp col -> typ -> Action tbl
append col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (ListAppend (nameGen col) attr)
    Nothing -> Nothing

prepend :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] ctyp col -> typ -> Action tbl
prepend col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (ListPrepend (nameGen col) attr)
    Nothing -> Nothing

add :: (InCollection col tbl 'FullPath, DynamoScalar typ)
    => Column (Set.Set typ) ctyp col -> typ -> Action tbl
add col val = Just $ Add (nameGen col) (dScalarEncode val)

delete :: (InCollection col tbl 'FullPath, DynamoScalar typ)
    => Column (Set.Set typ) ctyp col -> typ -> Action tbl
delete col val = Just $ Delete (nameGen col) (dScalarEncode val)
