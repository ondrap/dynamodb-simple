{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE OverloadedStrings #-}

module Database.DynamoDB.Update (
    Action
  , (+=.), (-=.), (=.)
  , setIfNotExists
  , append, prepend
  , add, delete
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

data Action t = Action [Set] [Add] [Delete] [Remove]

instance Semigroup (Action t) where
  Action a1 b1 c1 d1 <> Action a2 b2 c2 d2 = Action (a1 <> a2) (b1 <> b2) (c1 <> c2) (d1 <> d2)
instance Monoid (Action t) where
  mappend = (<>)
  mempty = Action [] [] [] []

isNoopAction :: Action t -> Bool
isNoopAction (Action [] [] [] []) = True
isNoopAction _ = False

data Set = Set NameGen ActionValue  -- General SET
data Add = Add NameGen AttributeValue -- Add value to a Set
data Delete = Delete NameGen AttributeValue -- Delete value from a Set
data Remove = Remove NameGen -- For Maybe types, remove attribute

class ActionClass a where
  dumpAction :: a -> Supply T.Text (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
  asAction :: a -> Action t
instance ActionClass Set where
  asAction a = Action [a] [] [] []
  dumpAction (Set name val) = do
    (subst, attrnames) <- name supplyName
    (expr, exprattr, valnames) <- mkActionVal val
    return (subst <> " = " <> expr, attrnames <> exprattr, valnames)
instance ActionClass Add where
  asAction a = Action [] [a] [] []
  dumpAction (Add name val) = do
    (subst, attrnames) <- name supplyName
    idval <- supplyValue
    let valnames = HMap.singleton idval val
    return (subst <> " " <> idval, attrnames, valnames)
instance ActionClass Delete where
  asAction a = Action [] [] [a] []
  dumpAction (Delete name val) = do
    (subst, attrnames) <- name supplyName
    idval <- supplyValue
    let valnames = HMap.singleton idval val
    return (subst <> " " <> idval, attrnames, valnames)
instance ActionClass Remove where
  asAction a = Action [] [] [] [a]
  dumpAction (Remove name) = do
    (subst, attrnames) <- name supplyName
    return (subst, attrnames, HMap.empty)


-- | Generate an action expression and associated structures from a list of actions
dumpActions :: Action t -> Maybe (T.Text, HashMap T.Text T.Text, HashMap T.Text AttributeValue)
dumpActions action@(Action iset iadd idelete iremove)
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

(+=.) :: (InCollection col tbl 'FullPath, DynamoScalar v typ, IsNumber typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(+=.) col val = asAction $ Set (nameGen col) (Plus (nameGen col) (dScalarEncode val))
infix 4 +=.

(-=.) :: (InCollection col tbl 'FullPath, DynamoScalar v typ, IsNumber typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(-=.) col val = asAction $ Set (nameGen col) (Minus (nameGen col) (dScalarEncode val))
infix 4 -=.

(=.) ::  (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
(=.) col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (ValAttr attr)
    Nothing -> asAction $ Remove (nameGen col)
infix 4 =.

setIfNotExists :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column typ 'TypColumn col -> typ -> Action tbl
setIfNotExists col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (IfNotExists (nameGen col) attr)
    Nothing -> mempty

append :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] 'TypColumn col -> typ -> Action tbl
append col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (ListAppend (nameGen col) attr)
    Nothing -> mempty

prepend :: (InCollection col tbl 'FullPath, DynamoEncodable typ)
    => Column [typ] 'TypColumn col -> typ -> Action tbl
prepend col val =
  case dEncode val of
    Just attr -> asAction $ Set (nameGen col) (ListPrepend (nameGen col) attr)
    Nothing -> mempty

add :: (InCollection col tbl 'FullPath, DynamoEncodable (Set.Set typ))
    => Column (Set.Set typ) 'TypColumn col -> Set.Set typ -> Action tbl
add col val
  | Set.null val = mempty
  | otherwise = maybe mempty (asAction . Add (nameGen col)) (dEncode val)

delete :: (InCollection col tbl 'FullPath, DynamoEncodable (Set.Set typ))
    => Column (Set.Set typ) 'TypColumn col -> Set.Set typ -> Action tbl
delete col val
  | Set.null val = mempty
  | otherwise = maybe mempty (asAction . Delete (nameGen col)) (dEncode val)
