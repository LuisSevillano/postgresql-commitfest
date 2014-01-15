/*-------------------------------------------------------------------------
 *
 * deparse_utility.c
 *	  Functions to convert utility commands back to command strings
 *
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/tcop/deparse_utility.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/heap.h"
#include "catalog/index.h"
#include "catalog/namespace.h"
#include "catalog/pg_class.h"
#include "catalog/pg_inherits.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "funcapi.h"
#include "lib/ilist.h"
#include "lib/stringinfo.h"
#include "nodes/makefuncs.h"
#include "nodes/parsenodes.h"
#include "parser/analyze.h"
#include "parser/parse_collate.h"
#include "parser/parse_expr.h"
#include "parser/parse_relation.h"
#include "parser/parse_type.h"
#include "tcop/deparse_utility.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/json.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/syscache.h"


typedef enum
{
	ParamTypeNull,
	ParamTypeBoolean,
	ParamTypeString,
	ParamTypeArray,
	ParamTypeObject
} ParamType;

typedef struct deparseParam
{
	char   *name;
	ParamType type;
	bool	bool_value;
	char   *str_value;
	struct deparseParams *obj_value;
	List   *array_value;
	slist_node node;
} deparseParam;

typedef struct deparseParams
{
	MemoryContext cxt;
	slist_head	params;
	int			numParams;
} deparseParams;

/*
 * Allocate a new object to store parameters.  If parent is NULL, a new memory
 * context is created for all allocations involving the parameters; if it's not
 * null, then the memory context from the given object is used.
 */
static deparseParams *
setup_params(deparseParams *parent)
{
	MemoryContext	cxt;
	deparseParams  *params;

	if (parent == NULL)
	{
		cxt = AllocSetContextCreate(CurrentMemoryContext,
									"deparse parameters",
									ALLOCSET_DEFAULT_MINSIZE,
									ALLOCSET_DEFAULT_INITSIZE,
									ALLOCSET_DEFAULT_MAXSIZE);
	}
	else
		cxt = parent->cxt;

	params = MemoryContextAlloc(cxt, sizeof(deparseParams));
	params->cxt = cxt;
	params->numParams = 0;
	slist_init(&params->params);

	return params;
}

/*
 * Add a new parameter with a NULL value
 */
static void
append_null_param(deparseParams *params, char *name)
{
	deparseParam	*param;

	param = MemoryContextAllocZero(params->cxt, sizeof(deparseParam));

	param->name = MemoryContextStrdup(params->cxt, name);
	param->type = ParamTypeNull;

	slist_push_head(&params->params, &param->node);
	params->numParams++;
}

/*
 * Add a new boolean parameter
 */
static void
append_boolean_param(deparseParams *params, char *name, bool value)
{
	deparseParam	*param;

	param = MemoryContextAllocZero(params->cxt, sizeof(deparseParam));

	param->name = MemoryContextStrdup(params->cxt, name);
	param->type = ParamTypeBoolean;
	param->bool_value = value;

	slist_push_head(&params->params, &param->node);
	params->numParams++;
}

/*
 * Add a new string parameter.
 */
static void
append_string_param(deparseParams *params, char *name, char *value)
{
	deparseParam	*param;

	param = MemoryContextAllocZero(params->cxt, sizeof(deparseParam));

	param->name = MemoryContextStrdup(params->cxt, name);
	param->type = ParamTypeString;
	param->str_value = MemoryContextStrdup(params->cxt, value);

	slist_push_head(&params->params, &param->node);
	params->numParams++;
}

/*
 * Add a new JSON parameter
 */
static void
append_object_param(deparseParams *params, char *name, deparseParams *value)
{
	deparseParam	*param;

	param = MemoryContextAllocZero(params->cxt, sizeof(deparseParam));

	param->name = MemoryContextStrdup(params->cxt, name);
	param->type = ParamTypeObject;
	param->obj_value = value;	/* XXX not duped */

	slist_push_head(&params->params, &param->node);
	params->numParams++;
}

/*
 * add a new array parameter
 */
static void
append_array_param(deparseParams *params, char *name, List *array)
{
	deparseParam   *param;

	param = MemoryContextAllocZero(params->cxt, sizeof(deparseParam));

	param->name = MemoryContextStrdup(params->cxt, name);
	param->type = ParamTypeArray;
	param->array_value = array;	/* XXX not duped */

	slist_push_head(&params->params, &param->node);
	params->numParams++;
}

/*
 * Release all memory used by parameters and their expansion
 */
static void
free_params(deparseParams *params)
{
	MemoryContextDelete(params->cxt);
}

/*
 * Create a true JSON object from our ad-hoc representation.
 *
 * Note this allocates memory in params->cxt.  We don't need a separate memory
 * context, and the one provided by the params object has the right lifetime.
 */
static char *
finalize_params(deparseParams *params)
{
	TupleDesc	tupdesc;
	MemoryContext oldcxt;
	Datum	   *values;
	bool	   *nulls;
	HeapTuple	htup;
	int			i;
	Datum		json;
	char	   *jsonstr;
	slist_iter	iter;

	oldcxt = MemoryContextSwitchTo(params->cxt);
	tupdesc = CreateTemplateTupleDesc(params->numParams, false);
	values = palloc(sizeof(Datum) * params->numParams);
	nulls = palloc(sizeof(bool) * params->numParams);

	i = 1;
	slist_foreach(iter, &params->params)
	{
		deparseParam *param = slist_container(deparseParam, node, iter.cur);
		Oid		typeid;

		switch (param->type)
		{
			case ParamTypeNull:
			case ParamTypeString:
				typeid = TEXTOID;
				break;
			case ParamTypeBoolean:
				typeid = BOOLOID;
				break;
			case ParamTypeArray:
			case ParamTypeObject:
				typeid = JSONOID;
				break;
			default:
				elog(ERROR, "unable to determine type id");
				typeid = InvalidOid;
		}

		TupleDescInitEntry(tupdesc, i, param->name, typeid, -1, 0);

		nulls[i - 1] = false;
		switch (param->type)
		{
			case ParamTypeNull:
				nulls[i - 1] = true;
				break;
			case ParamTypeBoolean:
				values[i - 1] = BoolGetDatum(param->bool_value);
				break;
			case ParamTypeString:
				values[i - 1] = CStringGetTextDatum(param->str_value);
				break;
			case ParamTypeArray:
				{
					ArrayType  *arrayt;
					Datum	   *arrvals;
					Datum		jsonary;
					ListCell   *cell;
					int			length = list_length(param->array_value);
					int			j;

					/*
					 * Arrays are stored as Lists up to this point, with each
					 * element being a deparseParam; we need to construct an
					 * ArrayType with them to turn the whole thing into a JSON
					 * array.
					 */
					j = 0;
					arrvals = palloc(sizeof(Datum) * length);
					foreach(cell, param->array_value)
					{
						deparseParams *json = lfirst(cell);

						arrvals[j++] =
							CStringGetTextDatum(finalize_params(json));
					}
					arrayt = construct_array(arrvals, length,
											 JSONOID, -1, false, 'i');

					jsonary = DirectFunctionCall1(array_to_json,
												  (PointerGetDatum(arrayt)));

					values[i - 1] = jsonary;
				}
				break;
			case ParamTypeObject:
				values[i - 1] =
					CStringGetTextDatum(finalize_params(param->obj_value));
				break;
		}

		i++;
	}

	BlessTupleDesc(tupdesc);
	htup = heap_form_tuple(tupdesc, values, nulls);
	json = DirectFunctionCall1(row_to_json, HeapTupleGetDatum(htup));

	/* switch to caller's context so that our output is allocated there */
	MemoryContextSwitchTo(oldcxt);

	jsonstr = TextDatumGetCString(json);

	return jsonstr;
}

/*
 * A helper routine to be used to setup %{}T elements.
 */
static deparseParams *
setup_params_for_type(deparseParams *parent, Oid typeId, int32 typmod)
{
	deparseParams *typeParam = setup_params(parent);
	bool	is_system;
	char   *typnsp;
	char   *typename;
	char   *typmodstr;
	bool	is_array;

	format_type_detailed(typeId, typmod,
						 &is_system, &typnsp, &typename, &typmodstr, &is_array);

	append_boolean_param(typeParam, "is_array", is_array);
	append_boolean_param(typeParam, "is_system", is_system);

	append_string_param(typeParam, "typename", typename);
	if (!is_system)
		append_string_param(typeParam, "schema", typnsp);
	append_string_param(typeParam, "typmod", typmodstr);

	return typeParam;
}

/*
 * A helper routine to be used to setup %{}D elements.
 */
static deparseParams *
setup_params_for_qualname(deparseParams *parent, Oid nspid, char *name)
{
	deparseParams *qualified = setup_params(parent);
	char   *namespace;

	namespace = get_namespace_name(nspid);
	append_string_param(qualified, "schema", namespace);
	append_string_param(qualified, "relation", name);

	pfree(namespace);

	return qualified;
}

/*
 * Given a raw expression used for a DEFAULT or a CHECK constraint, turn it
 * back into source code.
 *
 * Note we don't apply sanity checks such as on the return type of the
 * expression; since the expression was already run through the regular
 * code paths, we assume it's correct.
 */
static char *
uncookConstraintOrDefault(Node *raw_expr, ParseExprKind kind,
						  RangeVar *relation, Oid relOid)
{
	Node    *expr;
	char	*src;
	List	*dpcontext = NULL;
	ParseState *pstate;
	MemoryContext cxt;
	MemoryContext oldcxt;

	cxt = AllocSetContextCreate(CurrentMemoryContext,
								"uncook context",
								ALLOCSET_SMALL_MINSIZE,
								ALLOCSET_SMALL_INITSIZE,
								ALLOCSET_SMALL_MAXSIZE);
	oldcxt = MemoryContextSwitchTo(cxt);

	pstate = make_parsestate(NULL);
	if (relation)
	{
		RangeTblEntry *rte;

		rte = addRangeTableEntry(pstate, relation, NULL, false, true);
		addRTEtoQuery(pstate, rte, true, true, true);
		dpcontext = deparse_context_for(relation->relname, relOid);
	}

	/*
	 * Transform raw parsetree to executable expression, and apply collations
	 * as necessary.
	 */
	expr = transformExpr(pstate, raw_expr, kind);
	assign_expr_collations(pstate, expr);

	/*
	 * Finally, produce the requested expression, making sure it's allocated
	 * in the destination memory context.
	 */
	MemoryContextSwitchTo(oldcxt);
	src = deparse_expression(expr, dpcontext, false, false);

	/* and free resources */
	free_parsestate(pstate);
	MemoryContextDelete(cxt);

	return src;
}

/*
 * deparseColumnConstraint
 * 		deparse a T_Constraint node as it appears in a column definition
 */
static deparseParams *
deparseColumnConstraint(deparseParams *parent, RangeVar *rangevar, Oid relOid,
						Constraint *node)
{
	deparseParams *constraint;

	constraint = setup_params(parent);
	switch (node->contype)
	{
		case CONSTR_NOTNULL:
			append_string_param(constraint, "fmt",
								"%{name} NOT NULL");
			break;

		case CONSTR_NULL:
			/* ..?? */
			break;

		case CONSTR_UNIQUE:
			append_string_param(constraint, "fmt",
								"UNIQUE");
			/* _rwOptConsTableSpace(buf, c->indexspace); */
			break;

		case CONSTR_PRIMARY:
			append_string_param(constraint, "fmt",
								"%{name} PRIMARY KEY (%{columns}s)");
			break;

		case CONSTR_CHECK:
			{
				char *src;

				src = uncookConstraintOrDefault(node->raw_expr,
												EXPR_KIND_CHECK_CONSTRAINT,
												rangevar,
												relOid);
				append_string_param(constraint, "fmt",
									"CHECK (%{constraint})");
				append_string_param(constraint, "constraint",
									src);
				pfree(src);
			}
			break;

		case CONSTR_DEFAULT:
			{
				char	*src = NULL;

				if (node->cooked_expr)
				{
					List   *dpcontext;
					Node   *expr = (Node *) stringToNode(node->cooked_expr);

					dpcontext = deparse_context_for(rangevar->relname,
													relOid);
					src = deparse_expression(expr, dpcontext, false, false);
				}
				else if (node->raw_expr)
				{
					/* deparse the default expression */
					src = uncookConstraintOrDefault(node->raw_expr,
													EXPR_KIND_COLUMN_DEFAULT,
													rangevar, relOid);
				}

				append_string_param(constraint, "fmt",
									"DEFAULT %{default}");
				append_string_param(constraint, "default", src);
			}

			break;

		case CONSTR_FOREIGN:
			break;

		default:
			/* unexpected */
			elog(WARNING, "constraint %d is not a column constraint",
				 node->contype);
			break;
	}

	return constraint;
}

/*
 * deparse a ColumnDef node
 *
 * This routine doesn't process the constraint nodes in the coldef; caller must
 * see to it.  (Constraints represented intrinsically in the ColDef node
 * itself, such as NOT NULL, are emitted here).
 */
static deparseParams *
deparse_ColumnDef(deparseParams *parent, Oid relid, ColumnDef *coldef)
{
	deparseParams *column;
	deparseParams *collation;
	HeapTuple	attrTup;
	Form_pg_attribute	attrForm;
	Oid			typid;
	int32		typmod;
	Oid			typcollation;

	/*
	 * Inherited columns without local definitions, and columns coming from OF
	 * TYPE clauses, must not be emitted.  XXX -- maybe it is useful to have
	 * them with "present = false" or such?
	 */
	if (!coldef->is_local || coldef->is_from_type)
		return NULL;

	attrTup = SearchSysCacheAttName(relid, coldef->colname);
	if (!HeapTupleIsValid(attrTup))
		elog(ERROR, "could not find cache entry for column \"%s\" of relation %u",
			 coldef->colname, relid);
	attrForm = (Form_pg_attribute) GETSTRUCT(attrTup);

	column = setup_params(parent);

	get_atttypetypmodcoll(relid, attrForm->attnum,
						  &typid, &typmod, &typcollation);

	append_string_param(column, "fmt",
						"%{name}I %{type}T %{collation}s");
	append_string_param(column, "name", coldef->colname);

	append_object_param(column, "type",
						setup_params_for_type(column, typid, typmod));

	collation = setup_params(parent);
	append_string_param(collation, "fmt", "COLLATE %{name}I");
	if (OidIsValid(typcollation))
	{
		char	*collname;

		collname = get_collation_name(attrForm->attcollation);
		append_string_param(collation, "name", collname);
		pfree(collname);
	}
	else
		append_boolean_param(collation, "present", false);
	append_object_param(column, "collation", collation);

	ReleaseSysCache(attrTup);

	return column;
}

/*
 * Subroutine for deparse_CreateStmt: deal with column elements
 */
static List *
deparseTableElements(List *elements, deparseParams *parent,
					 RangeVar *rangevar,
					 Oid relOid, List *tableElements)
{
	ListCell   *lc;

	foreach(lc, tableElements)
	{
		Node   *elt = (Node *) lfirst(lc);

		switch (nodeTag(elt))
		{
			case T_ColumnDef:
				{
					ColumnDef  *colDef = (ColumnDef *) elt;
					ListCell   *lc;
					deparseParams *column;

					column = deparse_ColumnDef(parent, relOid, colDef);

					if (column != NULL)
						elements = lappend(elements, column);

					/*
					 * A ColumnDef might contain constraints internally;
					 * process them, too.
					 */
					foreach(lc, colDef->constraints)
					{
						Constraint	*constr = lfirst(lc);
						deparseParams *constraint;

						constraint =
							deparseColumnConstraint(parent, rangevar,
													relOid,
													(Constraint *) constr);
						if (constraint)
							elements = lappend(elements, constraint);
					}
				}
				break;
			case T_Constraint:
				break;
			default:
				elog(ERROR, "invalid node type %d", nodeTag(elt));
		}
	}

	return elements;
}

static void
append_persistence_param(deparseParams *params, char *name, char persistence)
{
	switch (persistence)
	{
		case RELPERSISTENCE_TEMP:
			append_string_param(params, name, "TEMPORARY");
			break;
		case RELPERSISTENCE_UNLOGGED:
			append_string_param(params, name, "UNLOGGED");
			break;
		case RELPERSISTENCE_PERMANENT:
			append_string_param(params, name, "");
			break;
	}
}

/*
 * deparse_CreateStmt
 * 		Process CREATE TABLE statements
 */
static void
deparse_CreateStmt(Oid objectId, Node *parsetree, char **command)
{
	CreateStmt *node = (CreateStmt *) parsetree;
	Relation	relation = relation_open(objectId, AccessShareLock);
	RangeVar   *rangevar;
	deparseParams *createStmt;
	deparseParams *tmp;

	rangevar = makeRangeVar(get_namespace_name(relation->rd_rel->relnamespace),
							RelationGetRelationName(relation),
							-1);

	createStmt = setup_params(NULL);
	append_string_param(createStmt, "fmt",
						"CREATE %{persistence}s TABLE %{identity}D "
						"%{if_not_exists}s "
						"%{of_type}s (%{table_elements:, }s) "
						"%{inherits}s %{on_commit}s %{tablespace}s");

	/*
	 * missing: WITH, LIKE, table elements in the typed table case
	 */

	append_persistence_param(createStmt,
							 "persistence",
							 relation->rd_rel->relpersistence);

	tmp = setup_params_for_qualname(createStmt,
									relation->rd_rel->relnamespace,
									RelationGetRelationName(relation));
	append_object_param(createStmt, "identity", tmp);

	append_string_param(createStmt, "if_not_exists",
						node->if_not_exists ? "IF NOT EXISTS" : "");

	tmp = setup_params(createStmt);
	append_string_param(tmp, "fmt", "OF %{of_type}T");
	if (node->ofTypename)
	{
		deparseParams *of_type_type;

		append_boolean_param(tmp, "present", true);
		of_type_type = setup_params_for_type(createStmt,
											 relation->rd_rel->reloftype,
											 -1);
		append_object_param(tmp, "of_type", of_type_type);

		/*
		 * XXX typed tables can have "table elements" which need to be
		 * processed here
		 */
	}
	else
	{
		append_null_param(tmp, "of_type");
		append_boolean_param(tmp, "present", false);
	}
	append_object_param(createStmt, "of_type", tmp);

	append_array_param(createStmt, "table_elements",
					   deparseTableElements(NIL,
											createStmt,
											rangevar,
											objectId,
											node->tableElts));

	/*
	 * Add inheritance specification.  We cannot simply scan the list of
	 * parents from the parser node, because that may lack the actual qualified
	 * names of the parent relations.  Rather than trying to re-resolve them from
	 * the information in the parse node, it seems more accurate and convenient
	 * to grab it from pg_inherits.
	 */
	tmp = setup_params(createStmt);
	append_string_param(tmp, "fmt", "INHERITS (%{parents:, }D)");
	if (list_length(node->inhRelations) > 0)
	{
		List	   *parents = NIL;
		Relation	catalogRelation;
		SysScanDesc scan;
		ScanKeyData key;
		HeapTuple	tuple;

		catalogRelation = heap_open(InheritsRelationId, RowExclusiveLock);

		ScanKeyInit(&key,
					Anum_pg_inherits_inhrelid,
					BTEqualStrategyNumber, F_OIDEQ,
					ObjectIdGetDatum(objectId));

		scan = systable_beginscan(catalogRelation, InheritsRelidSeqnoIndexId,
								  true, NULL, 1, &key);

		while (HeapTupleIsValid(tuple = systable_getnext(scan)))
		{
			deparseParams *parent;
			Form_pg_inherits formInh = (Form_pg_inherits) GETSTRUCT(tuple);
			Relation	prel;

			prel = heap_open(formInh->inhparent, AccessShareLock);

			parent = setup_params_for_qualname(createStmt,
											   prel->rd_rel->relnamespace,
											   RelationGetRelationName(prel));

			heap_close(prel, AccessShareLock);

			parents = lappend(parents, parent);
		}

		systable_endscan(scan);
		heap_close(catalogRelation, RowExclusiveLock);

		append_array_param(tmp, "parents", parents);
		append_boolean_param(tmp, "present", true);
	}
	else
	{
		append_null_param(tmp, "parents");
		append_boolean_param(tmp, "present", false);
	}
	append_object_param(createStmt, "inherits", tmp);

	tmp = setup_params(createStmt);
	append_string_param(tmp, "fmt", "TABLESPACE %{tablespace}I");
	if (node->tablespacename)
		append_string_param(tmp, "tablespace", node->tablespacename);
	else
	{
		append_null_param(tmp, "tablespace");
		append_boolean_param(tmp, "present", false);
	}
	append_object_param(createStmt, "tablespace", tmp);

	tmp = setup_params(createStmt);
	append_string_param(tmp, "fmt", "ON COMMIT %{on_commit_value}s");
	switch (node->oncommit)
	{
		case ONCOMMIT_DROP:
			append_string_param(tmp, "on_commit_value", "DROP");
			break;

		case ONCOMMIT_DELETE_ROWS:
			append_string_param(tmp, "on_commit_value", "DELETE ROWS");
			break;

		case ONCOMMIT_PRESERVE_ROWS:
			append_string_param(tmp, "on_commit_value", "PRESERVE ROWS");
			break;

		case ONCOMMIT_NOOP:
			append_null_param(tmp, "on_commit_value");
			append_boolean_param(tmp, "present", false);
			break;
	}
	append_object_param(createStmt, "on_commit", tmp);

	*command = finalize_params(createStmt);

	free_params(createStmt);
	relation_close(relation, AccessShareLock);
}

/*
 * rewrite CreateSeqStmt parser production
 */
static void
deparse_CreateSeqStmt(Oid objectId, Node *parsetree, char **command)
{
	deparseParams *createSeqStmt;
	deparseParams *sequence;
	Relation relation = relation_open(objectId, AccessShareLock);

	createSeqStmt = setup_params(NULL);
	append_string_param(createSeqStmt, "fmt",
						"CREATE %{persistence}s SEQUENCE %{identity}D");

	append_persistence_param(createSeqStmt,
							 "persistence",
							 relation->rd_rel->relpersistence);

	sequence = setup_params_for_qualname(createSeqStmt,
										 relation->rd_rel->relnamespace,
										 RelationGetRelationName(relation));
	append_object_param(createSeqStmt, "sequence", sequence);

	/* XXX Sequence options need to be processed here */

	*command = finalize_params(createSeqStmt);

	free_params(createSeqStmt);
	relation_close(relation, AccessShareLock);
}

/*
 * deparse IndexStmt
 *		Process CREATE INDEX statements
 */
static void
deparse_IndexStmt(Oid objectId, Node *parsetree, char **command)
{
	IndexStmt	   *node  = (IndexStmt *) parsetree;
	deparseParams  *indexStmt;
	deparseParams  *table;
	Relation		idxrel;
	Relation		heaprel;

	if (node->primary || node->isconstraint)
	{
		/*
		 * indexes for PRIMARY KEY or other constraints are output separately;
		 * return empty here.
		 */
		*command = NULL;
		return;
	}

	idxrel = relation_open(objectId, AccessShareLock);
	heaprel = relation_open(idxrel->rd_index->indrelid, AccessShareLock);

	indexStmt = setup_params(NULL);
	append_string_param(indexStmt, "fmt",
						"CREATE %{unique}s INDEX %{concurrently}s %{name}I "
						"ON %{table}D");

	append_string_param(indexStmt, "unique",
						node->unique ? "UNIQUE" : "");

	append_string_param(indexStmt, "concurrently",
						node->concurrent ?  "CONCURRENTLY" : "");

	append_string_param(indexStmt, "name", RelationGetRelationName(idxrel));

	table = setup_params_for_qualname(indexStmt,
									  heaprel->rd_rel->relnamespace,
									  RelationGetRelationName(heaprel));
	append_object_param(indexStmt, "table", table);

	*command = finalize_params(indexStmt);
	free_params(indexStmt);

	heap_close(idxrel, AccessShareLock);
	heap_close(heaprel, AccessShareLock);
}

/*
 * deparse CREATE SCHEMA
 *
 * We don't process the schema elements here, because CreateSchemaCommand
 * passes them back to ProcessUtility; they get output separately.
 */
static void
deparse_CreateSchemaStmt(Oid objectId, Node *parsetree, char **command)
{
	CreateSchemaStmt	*node  = (CreateSchemaStmt *) parsetree;
	deparseParams  *createSchema;
	deparseParams  *auth;

	createSchema = setup_params(NULL);
	append_string_param(createSchema, "fmt",
						"CREATE SCHEMA %{if_not_exists}s %{name}I "
						"%{authorization}s");

	append_string_param(createSchema, "name", node->schemaname);
	append_string_param(createSchema, "if_not_exists",
						node->if_not_exists ? "IF NOT EXISTS" : "");

	auth = setup_params(createSchema);
	append_string_param(auth, "fmt", "AUTHORIZATION %{authorization_role}I");
	if (node->authid)
	{
		append_string_param(auth, "authorization_role", node->authid);
		append_boolean_param(auth, "present", true);
	}
	else
	{
		append_null_param(auth, "authorization_role");
		append_boolean_param(auth, "present", false);
	}
	append_object_param(createSchema, "authorization", auth);

	*command = finalize_params(createSchema);

	free_params(createSchema);
}

/*
 * Given a utility command parsetree and the OID of the corresponding object,
 * return a JSON representation of the command.
 *
 * The command is expanded fully, so that there are no ambiguities even in the
 * face of search_path changes.
 *
 * Note we currently only support commands for which ProcessUtilitySlow saves
 * objects to create; currently this excludes all forms of ALTER and DROP.
 */
void
deparse_utility_command(Oid objectId, Node *parsetree, char **command)
{
	switch (nodeTag(parsetree))
	{
		case T_CreateSchemaStmt:
			deparse_CreateSchemaStmt(objectId, parsetree, command);
			break;

		case T_CreateStmt:
			deparse_CreateStmt(objectId, parsetree, command);

		case T_CreateForeignTableStmt:
			break;

		case T_DefineStmt:
			break;

		case T_IndexStmt:
			deparse_IndexStmt(objectId, parsetree, command);
			break;

		case T_CreateExtensionStmt:
			break;

		case T_CreateFdwStmt:
			break;

		case T_CreateForeignServerStmt:
			break;

		case T_CreateUserMappingStmt:
			break;

		case T_CompositeTypeStmt:	/* CREATE TYPE (composite) */
		case T_CreateEnumStmt:		/* CREATE TYPE AS ENUM */
		case T_CreateRangeStmt:		/* CREATE TYPE AS RANGE */
			break;

		case T_ViewStmt:
			break;

		case T_CreateFunctionStmt:
			break;

		case T_RuleStmt:
			break;

		case T_CreateSeqStmt:
			deparse_CreateSeqStmt(objectId, parsetree, command);
			break;

		case T_CreateTableAsStmt:
			break;

		case T_RefreshMatViewStmt:
			break;

		case T_CreateTrigStmt:
			break;

		case T_CreatePLangStmt:
			break;

		case T_CreateDomainStmt:
			break;

		case T_CreateConversionStmt:
			break;

		case T_CreateCastStmt:
			break;

		case T_CreateOpClassStmt:
			break;

		case T_CreateOpFamilyStmt:
			break;

		default:
			elog(LOG, "unrecognized node type: %d",
				 (int) nodeTag(parsetree));
	}

	return;
}
