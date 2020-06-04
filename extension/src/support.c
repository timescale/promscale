/* This file and its contents are licensed under the Apache License 2.0.
 * Please see the included NOTICE for copyright information and
 * LICENSE for a copy of the license
 */

#include <postgres.h>
#include <fmgr.h>

#include <nodes/pathnodes.h>
#include <nodes/supportnodes.h>
#include <nodes/bitmapset.h>
#include <optimizer/optimizer.h>

#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

PG_FUNCTION_INFO_V1(make_call_subquery_support);
PG_FUNCTION_INFO_V1(gapfill_delta_transition);
PG_FUNCTION_INFO_V1(gapfill_increase_transition);
PG_FUNCTION_INFO_V1(gapfill_rate_transition);
PG_FUNCTION_INFO_V1(gapfill_delta_final);

/* linker hack to make sure the rust code is actually linked in */
Datum _ensure_functions_link(PG_FUNCTION_ARGS);
Datum
_ensure_functions_link(PG_FUNCTION_ARGS)
{
	gapfill_delta_transition(fcinfo);
	gapfill_increase_transition(fcinfo);
	gapfill_rate_transition(fcinfo);
	return gapfill_delta_final(fcinfo);
}

static bool
arg_can_be_put_into_subquery(Node *arg) {
	if(IsA(arg, Const))
		return true;

	if(IsA(arg, CoerceToDomain)) {
		CoerceToDomain *dom = castNode(CoerceToDomain, arg);
		return arg_can_be_put_into_subquery((Node *)dom->arg);
	}

	return false;
}

/*
 * This is a support function that optimizes calls to the supported function if
 * it's called with constant-like arguments. Such calls are transformed into a
 * subquery of the function call. This allows the planner to make this call
 * an InitPlan which is evaluated once per query instead of multiple times
 * (e.g. on every tuple when the function is used in a WHERE clause).
 * This should be used on any stable function that is often called with constant-like
 * arguments.
*/
Datum
make_call_subquery_support(PG_FUNCTION_ARGS)
{
	Node	   *rawreq = (Node *) PG_GETARG_POINTER(0);
	Node	   *ret = NULL;
	if (IsA(rawreq, SupportRequestSimplify))
	{
		TargetEntry *te;
		FuncExpr *f2;
		Query *query;
		SupportRequestSimplify *req = (SupportRequestSimplify *) rawreq;
		FuncExpr   *expr = req->fcall;
		ListCell *lc;
		SubLink    *sublink;
		if (req->root == NULL)
		{
			PG_RETURN_POINTER(NULL);
		}

		/*
		 * This prevents recursion of this optimization when the subselect is
		 * planned
		 */
		if (req->root->query_level > 1)
		{
			PG_RETURN_POINTER(NULL);
		}
		foreach(lc, expr->args)
		{
			/* Check that these are expressions that don't reference
			any vars, i.e. they are constants or expressions of constants */
			Node *arg = lfirst(lc);

			if(!arg_can_be_put_into_subquery(arg))
			{
				PG_RETURN_POINTER(NULL);
			}
		}
		req->root->parse->hasSubLinks = true;
		te = makeNode(TargetEntry);
		f2 = copyObject(expr);
		te->expr = (Expr *) f2;
		te->resno = 1;
		query = makeNode(Query);
		query->commandType = 1;
		query->jointree = makeNode(FromExpr);
		query->canSetTag = true;
		query->targetList = list_make1(te);
		sublink = makeNode(SubLink);
		sublink->subLinkType = EXPR_SUBLINK;
		sublink->subLinkId = 0;
		sublink->subselect = (Node *) query;
		ret = (Node *)sublink;
	}
	PG_RETURN_POINTER(ret);
}
