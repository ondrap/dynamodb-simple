{-# LANGUAGE CPP             #-}
{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE LambdaCase #-}

-- | Module generating 'toTable' class/instance for converting from index to main table type
module Database.DynamoDB.THConvert (
  createTableConversions
) where

import           Control.Monad                   (replicateM, when)
import           Control.Monad.Trans.Class       (lift)
import           Control.Monad.Trans.Writer.Lazy (WriterT, pass)
import           Data.List                       (elemIndex, sort)
import           Data.Monoid                     ((<>))
import           Language.Haskell.TH

import           Database.DynamoDB.THLens        (getFieldNames, say)

getConstructor :: Name -> Q Name
getConstructor tbl = do
    info <- reify tbl
    case info of
#if __GLASGOW_HASKELL__ >= 800
        (TyConI (DataD _ _ _ _ [RecC name _] _)) -> return name
#else
        (TyConI (DataD _ _ _ [RecC name _] _)) -> return name
#endif
        _ -> fail "not a record declaration with 1 constructor"

-- | Create function that converts complete structure from index back to table.
--
-- DynamoDB limits total number of attribute projections to 20, so this may
-- not be as useful as it appears.
createTableConversions :: (String -> String) -> Name -> [Name] -> WriterT [Dec] Q ()
createTableConversions translate table idxes = do
    tblFields <- getFieldNames table translate
    tblConstr <- lift $ getConstructor table
    clsname <- lift $ newName $ "IndexToTable_" <> nameBase tblConstr
    a <- lift $ newName "a"
    let clsdef = ClassD [] clsname [PlainTV a] [] [SigD funcname (AppT (AppT ArrowT (VarT a)) (ConT table))]
    let instth = mapM_ (mkInstance tblFields tblConstr clsname) idxes
    -- Create a typeclass only if something got created
    pass (instth >> return ((), \case {[] -> []; lst -> clsdef:lst}))
  where
    funcname = mkName ("to" <> nameBase table)

    mkInstance tblFields tblConstr clsname idxname = do
        let tblNames = map fst tblFields
        idxFields <- getFieldNames idxname translate
        let idxNames = map fst idxFields
        idxConstr <- lift $ getConstructor idxname

        when (sort tblNames == sort idxNames) $
          case mapM (`elemIndex` idxNames) tblNames of
              Nothing -> return ()
              Just varidxmap -> do
                  varnames <- lift $ replicateM (length idxNames) (newName "a")
                  let ivars = map varP varnames
                  let toJust = zipWith makeJust (map snd tblFields) (map (snd . (idxFields !!)) varidxmap)
                      olist = zipWith ($) toJust $ map (varnames !!) varidxmap
                      ovars = foldl appE (conE tblConstr) olist
                  let func = funD funcname [clause [conP idxConstr ivars] (normalB ovars) []]
                  lift (instanceD (pure []) (appT (conT clsname) (conT idxname))
                        [func]) >>= say

    makeJust (AppT (ConT mbtype) dsttype) srctype
        | mbtype == ''Maybe && dsttype == srctype = appE (conE 'Just) . varE
    makeJust _ _ = varE
