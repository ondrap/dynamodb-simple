{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.DynamoDb.Update (
    Action
  , (+=.), (-=.), (=.)
  , setIfNotExists
  , append, prepend
  , add, delete
  , dumpActions
) where

import           Control.Monad.Supply       (Supply, evalSupply, supply)
import           Data.Foldable              (foldl')
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.Maybe                 (catMaybes)
import           Data.Monoid                ((<>))
import qualified Data.Set                   as Set
import qualified Data.Text                  as T
import           Network.AWS.DynamoDB.Types (AttributeValue)

import           Database.DynamoDb.Internal
import           Database.DynamoDb.Types

data ActionValue =
    ValAttr AttributeValue
    | IfNotExists NameGen AttributeValue
    | ListAppend NameGen AttributeValue
    | ListPrepend NameGen AttributeValue
    | Plus NameGen AttributeValue -- Add number to existing value
    | Minus NameGen AttributeValue -- Subtract number from existing value

data Action' t =
      Set NameGen ActionValue  -- General SET
    | Add NameGen AttributeValue -- Add value to a Set
    | Delete NameGen AttributeValue -- Delete value from a Set
    | Remove NameGen -- For Maybe types, remove attribute


-- | Generate an action expression and associated structures from a list of actions
dumpActions :: [Action t] -> Maybe (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
dumpActions actions = evalSupply eval names
  where
    eval = do
      lst <- mapM dumpAction (catMaybes actions)
      case lst of
        (x:xs) -> return $ Just (foldl' (<>) x xs)
        [] -> return Nothing
    names = map (\i -> T.pack ("A" <> show i)) ([1..] :: [Int])


supplyName :: Supply T.Text T.Text
supplyName = ("#" <>) <$> supply
supplyValue :: Supply T.Text T.Text
supplyValue = (":" <> ) <$> supply

-- |
-- Note: we start the actions with a space so they can be easily folded in dumpActions
dumpAction :: Action' t -> Supply T.Text (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
dumpAction (Add name val) = do
  (subst, attrnames) <- name supplyName
  idval <- supplyValue
  let valnames = HMap.singleton idval val
  return (" ADD " <> subst <> " " <> idval, attrnames, valnames)
dumpAction (Delete name val) = do
  (subst, attrnames) <- name supplyName
  idval <- supplyValue
  let valnames = HMap.singleton idval val
  return (" DELETE " <> subst <> " " <> idval, attrnames, valnames)
dumpAction (Remove name) = do
  (subst, attrnames) <- name supplyName
  return (" REMOVE " <> subst, attrnames, HMap.empty)
dumpAction (Set name val) = do
  (subst, attrnames) <- name supplyName
  (expr, exprattr, valnames) <- mkActionVal val
  return (" SET " <> subst <> " = " <> expr, attrnames <> exprattr, valnames)

mkActionVal :: ActionValue -> Supply T.Text (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
mkActionVal (ValAttr val) = do
  valname <- supplyValue
  return (valname, HMap.empty, HMap.singleton valname val)
mkActionVal (IfNotExists name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return ("if_not_exists(" <> subst <> "," <> valname  <> ")", attrnames, HMap.singleton valname val)
mkActionVal (ListAppend name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return ("list_append(" <> valname <> "," <> subst <> ")", attrnames, HMap.singleton valname val)
mkActionVal (ListPrepend name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return ("list_append(" <> subst <> "," <> valname  <> ")", attrnames, HMap.singleton valname val)
mkActionVal (Plus name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return (subst <> "+" <> valname, attrnames, HMap.singleton valname val)
mkActionVal (Minus name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return (subst <> "-" <> valname, attrnames, HMap.singleton valname val)

-- | Type representing an action for updateItem
type Action t = Maybe (Action' t)

(+=.) :: (InCollection col tbl 'FullPath, DynamoScalar typ, IsNumber typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(+=.) col val = Just $ Set (nameGen col) (Plus (nameGen col) (dScalarEncode val))
infix 4 +=.

(-=.) :: (InCollection col tbl 'FullPath, DynamoScalar typ, IsNumber typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(-=.) col val = Just $ Set (nameGen col) (Minus (nameGen col) (dScalarEncode val))
infix 4 -=.

(=.) ::  (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(=.) col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (ValAttr attr)
    Nothing -> Just $ Remove (nameGen col)
infix 4 =.

setIfNotExists :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
setIfNotExists col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (IfNotExists (nameGen col) attr)
    Nothing -> Nothing

append :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] 'TypColumn col -> typ -> Action tbl
append col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (ListAppend (nameGen col) attr)
    Nothing -> Nothing

prepend :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] 'TypColumn col -> typ -> Action tbl
prepend col val =
  case dEncode val of
    Just attr -> Just $ Set (nameGen col) (ListPrepend (nameGen col) attr)
    Nothing -> Nothing

add :: (InCollection col tbl 'FullPath, DynamoEncodable (Set.Set typ))
    => Column (Set.Set typ) 'TypColumn col -> Set.Set typ -> Action tbl
add col val
  | Set.null val = Nothing
  | otherwise = Add (nameGen col) <$> dEncode val

delete :: (InCollection col tbl 'FullPath, DynamoEncodable (Set.Set typ))
    => Column (Set.Set typ) 'TypColumn col -> Set.Set typ -> Action tbl
delete col val
  | Set.null val = Nothing
  | otherwise = Delete (nameGen col) <$> dEncode val
