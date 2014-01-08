/*-------------------------------------------------------------------------
 *
 * genam.h
 *	  POSTGRES generalized index access method definitions.
 *
 *
 * Portions Copyright (c) 1996-2014, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/access/genam.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef GENAM_H
#define GENAM_H

#include "access/sdir.h"
#include "access/skey.h"
#include "nodes/tidbitmap.h"
#include "storage/lock.h"
#include "utils/relcache.h"
#include "utils/snapshot.h"

/*
 * Struct for statistics returned by ambuild
 */
typedef struct IndexBuildResult
{
	double		heap_tuples;	/* # of tuples seen in parent table */
	double		index_tuples;	/* # of tuples inserted into index */
} IndexBuildResult;

/*
 * Struct for input arguments passed to ambulkdelete and amvacuumcleanup
 *
 * num_heap_tuples is accurate only when estimated_count is false;
 * otherwise it's just an estimate (currently, the estimate is the
 * prior value of the relation's pg_class.reltuples field).  It will
 * always just be an estimate during ambulkdelete.
 */
typedef struct IndexVacuumInfo
{
	Relation	index;			/* the index being vacuumed */
	bool		analyze_only;	/* ANALYZE (without any actual vacuum) */
	bool		estimated_count;	/* num_heap_tuples is an estimate */
	int			message_level;	/* ereport level for progress messages */
	double		num_heap_tuples;	/* tuples remaining in heap */
	BufferAccessStrategy strategy;		/* access strategy for reads */
} IndexVacuumInfo;

/*
 * Struct for statistics returned by ambulkdelete and amvacuumcleanup
 *
 * This struct is normally allocated by the first ambulkdelete call and then
 * passed along through subsequent ones until amvacuumcleanup; however,
 * amvacuumcleanup must be prepared to allocate it in the case where no
 * ambulkdelete calls were made (because no tuples needed deletion).
 * Note that an index AM could choose to return a larger struct
 * of which this is just the first field; this provides a way for ambulkdelete
 * to communicate additional private data to amvacuumcleanup.
 *
 * Note: pages_removed is the amount by which the index physically shrank,
 * if any (ie the change in its total size on disk).  pages_deleted and
 * pages_free refer to free space within the index file.  Some index AMs
 * may compute num_index_tuples by reference to num_heap_tuples, in which
 * case they should copy the estimated_count field from IndexVacuumInfo.
 */
typedef struct IndexBulkDeleteResult
{
	BlockNumber num_pages;		/* pages remaining in index */
	BlockNumber pages_removed;	/* # removed during vacuum operation */
	bool		estimated_count;	/* num_index_tuples is an estimate */
	double		num_index_tuples;		/* tuples remaining */
	double		tuples_removed; /* # removed during vacuum operation */
	BlockNumber pages_deleted;	/* # unused pages in index */
	BlockNumber pages_free;		/* # pages available for reuse */
} IndexBulkDeleteResult;

/* Typedef for callback function to determine if a tuple is bulk-deletable */
typedef bool (*IndexBulkDeleteCallback) (ItemPointer itemptr, void *state);

/* struct definitions appear in relscan.h */
typedef struct IndexScanDescData *IndexScanDesc;
typedef struct SysScanDescData *SysScanDesc;

/*
 * Enumeration specifying the type of uniqueness check to perform in
 * index_insert().
 *
 * UNIQUE_CHECK_YES is the traditional Postgres immediate check, possibly
 * blocking to see if a conflicting transaction commits.
 *
 * For deferrable unique constraints, UNIQUE_CHECK_PARTIAL is specified at
 * insertion time.	The index AM should test if the tuple is unique, but
 * should not throw error, block, or prevent the insertion if the tuple
 * appears not to be unique.  We'll recheck later when it is time for the
 * constraint to be enforced.  The AM must return true if the tuple is
 * known unique, false if it is possibly non-unique.  In the "true" case
 * it is safe to omit the later recheck.
 *
 * When it is time to recheck the deferred constraint, a pseudo-insertion
 * call is made with UNIQUE_CHECK_EXISTING.  The tuple is already in the
 * index in this case, so it should not be inserted again.	Rather, just
 * check for conflicting live tuples (possibly blocking).
 *
 * INSERT...ON DUPLICATE KEY IGNORE/LOCK FOR UPDATE operations involve
 * "speculative" insertion of tuples.  There is a call to establish the
 * uniqueness of a tuple and take appropriate value locks (once per unique
 * index per table slot).  These locks prevent concurrent insertion of
 * conflicting key values, and will only be held for an instant.  Subsequently,
 * there may be a second, corresponding phase where insertion actually occurs.
 * Then, the potential index_insert() calls are made with UNIQUE_CHECK_SPEC.
 * However, even when speculative insertion is requested, non-unique indexes
 * only perform insertion in the second phase, in the conventional manner,
 * because UNIQUE_CHECK_NO is passed to the AM.
 */
typedef enum IndexUniqueCheck
{
	UNIQUE_CHECK_NO,			/* Don't do any uniqueness checking */
	UNIQUE_CHECK_YES,			/* Enforce uniqueness at insertion time */
	UNIQUE_CHECK_PARTIAL,		/* Test uniqueness, but no error */
	UNIQUE_CHECK_EXISTING,		/* Check if existing tuple is unique */
	UNIQUE_CHECK_SPEC			/* Speculative phased locking insertion */
} IndexUniqueCheck;

/*
 * For speculative insertion's phased locking, it is necessary for the core
 * system to have callbacks to amcanunique AMs that allow them to clean-up in
 * the duplicate found case.  This is because one first-phase call in respect
 * of some unique index might indicate that it's okay to proceed, while another
 * such call relating to another unique index (but the same executor-level
 * tuple slot) indicates that it is not.  In general, we cannot rely on
 * actually reaching the second phase even if some check in the first phase
 * says that it thinks insertion ought to proceed, because we need total
 * consensus from all unique indexes that may be inserted into as part of
 * proceeding with inserting the tuple slot.
 *
 * It is the responsibility of supporting AMs to set the callback if there is
 * clean-up to be done when not proceeding.	 Note, however, that the executor
 * will not call the callback if insertion of the relevant index tuple
 * proceeds, because the AM should take care of this itself in the second,
 * final, optional step ("insertion proper").
 */
struct SpeculativeState;
typedef void (*ReleaseIndexCallback) (struct SpeculativeState *state);

/*
 * Speculative status returned.
 *
 * It may be necessary to start the first phase of speculative insertion from
 * scratch, in the event of needing to block pending the completion of another
 * transaction, where the AM cannot reasonably sit on value locks (associated
 * with some other, previously locked amcanunique indexes) indefinitely.
 */
typedef enum
{
	INSERT_TRY_AGAIN,	/* had xact conflict - restart from first index */
	INSERT_NO_PROCEED,	/* duplicate conclusively found */
	INSERT_PROCEED		/* no duplicate key in any index */
} SpecStatus;

/*
 * Struct representing state passed to and from clients for speculative phased
 * insertion.  This is allocated by amcanunique access methods, and passed back
 * and forth for phased index locking.
 *
 * There is one such piece of state associated with every unique index
 * participating in insertion of a slot.
 */
typedef struct SpeculativeState
{
	/* Index under consideration */
	Relation				uniqueIndex;
	/* Opaque amcanunique state */
	void				   *privState;
	/* Callback to tell AM to clean-up */
	ReleaseIndexCallback	callback;
	/* Outcome of first phase (current attempt) for uniqueIndex */
	SpecStatus				outcome;
	/* Conflicting heap tuple, if any */
	ItemPointer				conflictTid;
}	SpeculativeState;

/*
 * generalized index_ interface routines (in indexam.c)
 */

/*
 * IndexScanIsValid
 *		True iff the index scan is valid.
 */
#define IndexScanIsValid(scan) PointerIsValid(scan)

extern Relation index_open(Oid relationId, LOCKMODE lockmode);
extern void index_close(Relation relation, LOCKMODE lockmode);

extern bool index_insert(Relation indexRelation,
			 Datum *values, bool *isnull,
			 ItemPointer heap_t_ctid,
			 Relation heapRelation,
			 IndexUniqueCheck checkUnique,
			 SpeculativeState * state);
extern SpeculativeState *index_lock(Relation indexRelation,
		   Datum *values,
		   bool *isnull,
		   Relation heapRelation,
		   List *otherSpecStates,
		   bool priorConflict);
extern bool index_proceed(List *specStates);
extern ItemPointer index_release(List *specStates);

extern IndexScanDesc index_beginscan(Relation heapRelation,
				Relation indexRelation,
				Snapshot snapshot,
				int nkeys, int norderbys);
extern IndexScanDesc index_beginscan_bitmap(Relation indexRelation,
					   Snapshot snapshot,
					   int nkeys);
extern void index_rescan(IndexScanDesc scan,
			 ScanKey keys, int nkeys,
			 ScanKey orderbys, int norderbys);
extern void index_endscan(IndexScanDesc scan);
extern void index_markpos(IndexScanDesc scan);
extern void index_restrpos(IndexScanDesc scan);
extern ItemPointer index_getnext_tid(IndexScanDesc scan,
				  ScanDirection direction);
extern HeapTuple index_fetch_heap(IndexScanDesc scan);
extern HeapTuple index_getnext(IndexScanDesc scan, ScanDirection direction);
extern int64 index_getbitmap(IndexScanDesc scan, TIDBitmap *bitmap);

extern IndexBulkDeleteResult *index_bulk_delete(IndexVacuumInfo *info,
				  IndexBulkDeleteResult *stats,
				  IndexBulkDeleteCallback callback,
				  void *callback_state);
extern IndexBulkDeleteResult *index_vacuum_cleanup(IndexVacuumInfo *info,
					 IndexBulkDeleteResult *stats);
extern bool index_can_return(Relation indexRelation);
extern RegProcedure index_getprocid(Relation irel, AttrNumber attnum,
				uint16 procnum);
extern FmgrInfo *index_getprocinfo(Relation irel, AttrNumber attnum,
				  uint16 procnum);

/*
 * index access method support routines (in genam.c)
 */
extern IndexScanDesc RelationGetIndexScan(Relation indexRelation,
					 int nkeys, int norderbys);
extern void IndexScanEnd(IndexScanDesc scan);
extern char *BuildIndexValueDescription(Relation indexRelation,
						   Datum *values, bool *isnull);

/*
 * heap-or-index access to system catalogs (in genam.c)
 */
extern SysScanDesc systable_beginscan(Relation heapRelation,
				   Oid indexId,
				   bool indexOK,
				   Snapshot snapshot,
				   int nkeys, ScanKey key);
extern HeapTuple systable_getnext(SysScanDesc sysscan);
extern bool systable_recheck_tuple(SysScanDesc sysscan, HeapTuple tup);
extern void systable_endscan(SysScanDesc sysscan);
extern SysScanDesc systable_beginscan_ordered(Relation heapRelation,
						   Relation indexRelation,
						   Snapshot snapshot,
						   int nkeys, ScanKey key);
extern HeapTuple systable_getnext_ordered(SysScanDesc sysscan,
						 ScanDirection direction);
extern void systable_endscan_ordered(SysScanDesc sysscan);

#endif   /* GENAM_H */
