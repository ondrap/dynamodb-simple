{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}

-- | Module for creating update actions
--
-- Example as used in nested structure for scan:
--
-- > updateItemByKey_ (Proxy :: Proxy Test, ("hashkey", "sortkey"))
-- >                  ((colIInt +=. 5) <> (colIText =. "updated") <> (colIMText =. Nothing))
--
-- The unique "Action" can be added together using the "<>" operator. You are not supposed
-- to operate on the same structure simultaneously using multiple commands.
module Database.DynamoDB.Update (
    Action
    -- * Update action
  , (+=.), (-=.), (=.)
  , setIfNothing
  , append, prepend
  , add, delete
  , delListItem
  , delHashKey
  -- * Utility function
  , dumpActions
) where

import           Control.Lens               (over, _1)
import           Control.Monad.Supply       (Supply, evalSupply, supply)
import           Data.HashMap.Strict        (HashMap)
import qualified Data.HashMap.Strict        as HMap
import           Data.Semigroup
import qualified Data.Set                   as Set
import qualified Data.Text                  as T
import           Network.AWS.DynamoDB.Types (AttributeValue)

import           Database.DynamoDB.Internal
import           Database.DynamoDB.Types

data ActionValue =
    ValAttr AttributeValue
    | IfNotExists NameGen AttributeValue
    | ListAppend NameGen AttributeValue
    | ListPrepend NameGen AttributeValue
    | Plus NameGen AttributeValue -- Add number to existing value
    | Minus NameGen AttributeValue -- Subtract number from existing value

-- | An action for UpdateItem functions.
newtype Action t = Action ([Set], [Add], [Delete], [Remove])
  deriving (Semigroup, Monoid)

isNoopAction :: Action t -> Bool
isNoopAction (Action ([], [], [], [])) = True
isNoopAction _ = False

data Set = Set NameGen ActionValue  -- General SET
data Add = Add NameGen AttributeValue -- Add value to a Set
data Delete = Delete NameGen AttributeValue -- Delete value from a Set
data Remove = Remove NameGen -- For Maybe types, remove attribute

class ActionClass a where
  dumpAction :: a -> Supply T.Text (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
  asAction :: a -> Action t
instance ActionClass Set where
  asAction a = Action ([a], [], [], [])
  dumpAction (Set name val) = do
    (subst, attrnames) <- name supplyName
    (expr, exprattr, valnames) <- mkActionVal val
    return (subst <> " = " <> expr, attrnames <> exprattr, valnames)
instance ActionClass Add where
  asAction a = Action ([], [a], [], [])
  dumpAction (Add name val) = do
    (subst, attrnames) <- name supplyName
    idval <- supplyValue
    let valnames = HMap.singleton idval val
    return (subst <> " " <> idval, attrnames, valnames)
instance ActionClass Delete where
  asAction a = Action ([], [], [a], [])
  dumpAction (Delete name val) = do
    (subst, attrnames) <- name supplyName
    idval <- supplyValue
    let valnames = HMap.singleton idval val
    return (subst <> " " <> idval, attrnames, valnames)
instance ActionClass Remove where
  asAction a = Action ([], [], [], [a])
  dumpAction (Remove name) = do
    (subst, attrnames) <- name supplyName
    return (subst, attrnames, HMap.empty)


-- | Generate an action expression and associated structures from a list of actions
dumpActions :: Action t -> Maybe (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
dumpActions action@(Action (iset, iadd, idelete, iremove))
  | isNoopAction action = Nothing
  | otherwise = Just $ evalSupply eval nameSupply
  where
    eval :: Supply T.Text (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
    eval = do
      dset <- mksection "SET" <$> mapM dumpAction iset
      dadd <- mksection "ADD" <$> mapM dumpAction iadd
      ddelete <- mksection "DELETE" <$> mapM dumpAction idelete
      dremove <- mksection "REMOVE" <$> mapM dumpAction iremove
      return $ dset <> dadd <> ddelete <> dremove
    mksection :: T.Text -> [(T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)] -> (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
    mksection _ [] = ("", HMap.empty, HMap.empty)
    mksection secname xs =
      let (exprs, attnames, attvals) = mconcat $ map (over _1 (: [])) xs
      in (" " <> secname <> " " <> T.intercalate "," exprs, attnames, attvals)
    nameSupply = map (\i -> T.pack ("A" <> show i)) ([1..] :: [Int])


supplyName :: Supply T.Text T.Text
supplyName = ("#" <>) <$> supply
supplyValue :: Supply T.Text T.Text
supplyValue = (":" <> ) <$> supply


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
  return ("list_append(" <> subst <> "," <> valname  <> ")", attrnames, HMap.singleton valname val)
mkActionVal (ListPrepend name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return ("list_append(" <> valname <> "," <> subst <> ")", attrnames, HMap.singleton valname val)
mkActionVal (Plus name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return (subst <> "+" <> valname, attrnames, HMap.singleton valname val)
mkActionVal (Minus name val) = do
  valname <- supplyValue
  (subst, attrnames) <- name supplyName
  return (subst <> "-" <> valname, attrnames, HMap.singleton valname val)

-- | Add a number to a saved attribute.
(+=.) :: (InCollection col tbl 'FullPath, DynamoScalar v typ, IsNumber typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(+=.) col val = asAction $ Set (nameGen col) (Plus (nameGen col) (dScalarEncode val))
infix 4 +=.

-- | Subtract a number from a saved attribute.
(-=.) :: (InCollection col tbl 'FullPath, DynamoScalar v typ, IsNumber typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(-=.) col val = asAction $ Set (nameGen col) (Minus (nameGen col) (dScalarEncode val))
infix 4 -=.

-- | Set an attribute to a new value.
(=.) ::  (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(=.) col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (ValAttr attr)
    Nothing -> asAction $ Remove (nameGen col)
infix 4 =.

-- | Set on a Maybe type, if it was not set before.
setIfNothing :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column (Maybe typ) 'TypColumn col -> typ -> Action tbl
setIfNothing col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (IfNotExists (nameGen col) attr)
    Nothing -> mempty

-- | Append a new value to an end of a list.
append :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] 'TypColumn col -> [typ] -> Action tbl
append col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (ListAppend (nameGen col) attr)
    Nothing -> mempty

-- | Insert a value to a beginning of a list
prepend :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] 'TypColumn col -> [typ] -> Action tbl
prepend col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (ListPrepend (nameGen col) attr)
    Nothing -> mempty

-- | Add a new value to a set.
add :: (InCollection col tbl 'FullPath, DynamoEncodable (Set.Set typ))
    => Column (Set.Set typ) 'TypColumn col -> Set.Set typ -> Action tbl
add col val
  | Set.null val = mempty
  | otherwise = maybe mempty (asAction . Add (nameGen col)) (dEncode val)

-- | Remove a value from a set.
delete :: (InCollection col tbl 'FullPath, DynamoEncodable (Set.Set typ))
    => Column (Set.Set typ) 'TypColumn col -> Set.Set typ -> Action tbl
delete col val
  | Set.null val = mempty
  | otherwise = maybe mempty (asAction . Delete (nameGen col)) (dEncode val)

-- | Delete n-th list of an item.
delListItem :: InCollection col tbl 'FullPath
    => Column [typ] 'TypColumn col -> Int -> Action tbl
delListItem col idx = asAction $ Remove (nameGen (col <!> idx))

-- | Delete a key from a map.
delHashKey :: (InCollection col tbl 'FullPath, IsText key)
    => Column (HashMap key typ) 'TypColumn col -> key -> Action tbl
delHashKey col key = asAction $ Remove (nameGen (col <!:> key))
