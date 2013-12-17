/*-------------------------------------------------------------------------
 *
 * network_gist.c
 *	  GiST support for network types.
 *
 * Portions Copyright (c) 1996-2013, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/utils/adt/network_gist.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/gist.h"
#include "access/skey.h"
#include "utils/inet.h"

/*
 * The GiST query consistency check
 */
Datum
inet_gist_consistent(PG_FUNCTION_ARGS)
{
	GISTENTRY	   *ent = (GISTENTRY *) PG_GETARG_POINTER(0);
	inet		   *orig = DatumGetInetP(ent->key),
				   *query = PG_GETARG_INET_PP(1);
	StrategyNumber  strategy = (StrategyNumber) PG_GETARG_UINT16(2);
	/* Oid 			subtype = PG_GETARG_OID(3); */
	bool	   	   *recheck = (bool *) PG_GETARG_POINTER(4);
	int 			minbits,
					order;

	/* All operators served by this function are exact. */
	*recheck = false;

	/*
	 * Check 0: different families
	 *
	 * 0 is the special number for the family field. It means sub nodes
	 * include networks with different address families. The index should
	 * only have this node on the top. Proper inet type has no chance
	 * to have 0 on the family field.
	 */
	if (ip_family(orig) == 0)
		PG_RETURN_BOOL(true);

	/*
	 * Check 1: different families
	 *
	 * Matching families do not help any of the strategies.
	 */
	if (ip_family(orig) != ip_family(query))
	{
		switch (strategy)
		{
			case INETSTRAT_LT:
			case INETSTRAT_LE:
				if (ip_family(orig) < ip_family(query))
					PG_RETURN_BOOL(true);
				break;

			case INETSTRAT_GE:
			case INETSTRAT_GT:
				if (ip_family(orig) > ip_family(query))
					PG_RETURN_BOOL(true);
				break;
		}

		PG_RETURN_BOOL(false);
	}

	/*
	 * Check 2: network bit count
	 *
	 * Network bit count (ip_bits) helps to check leaves for sub network
	 * and sup network operators.
	 */
	switch (strategy)
	{
		case INETSTRAT_SUB:
			if (GIST_LEAF(ent) && ip_bits(orig) <= ip_bits(query))
				PG_RETURN_BOOL(false);
			break;

		case INETSTRAT_SUBEQ:
			if (GIST_LEAF(ent) && ip_bits(orig) < ip_bits(query))
				PG_RETURN_BOOL(false);
			break;

		case INETSTRAT_SUPEQ:
			if (ip_bits(orig) > ip_bits(query))
				PG_RETURN_BOOL(false);
			break;

		case INETSTRAT_SUP:
			if (ip_bits(orig) >= ip_bits(query))
				PG_RETURN_BOOL(false);
			break;
	}

	/*
	 * Check 3: empty address
	 *
	 * If there are not any possible common bits, do not go futher
	 * return true as the leaves under this node can contain any address.
	 */
	minbits = Min(ip_bits(orig), ip_bits(query));

	if (minbits == 0)
	{
		switch (strategy)
		{
			case INETSTRAT_SUB:
			case INETSTRAT_SUBEQ:
			case INETSTRAT_OVERLAPS:
			case INETSTRAT_SUPEQ:
			case INETSTRAT_SUP:
				PG_RETURN_BOOL(true);
		}

		if (!GIST_LEAF(ent))
			PG_RETURN_BOOL(true);
	}

	/*
	 * Check 4: common network bits
	 *
	 * Common network bits is the final check for operators which
	 * only consider the network part of the address.
	 */
	if (minbits > 0)
	{
		order = bitncmp(ip_addr(orig), ip_addr(query), minbits);

		switch (strategy)
		{
			case INETSTRAT_SUB:
			case INETSTRAT_SUBEQ:
			case INETSTRAT_OVERLAPS:
			case INETSTRAT_SUPEQ:
			case INETSTRAT_SUP:
				PG_RETURN_BOOL(order == 0);

			case INETSTRAT_LT:
			case INETSTRAT_LE:
				if (order > 0)
					PG_RETURN_BOOL(false);
				if (order < 0 || !GIST_LEAF(ent))
					PG_RETURN_BOOL(true);
				break;

			case INETSTRAT_EQ:
				if (order != 0)
					PG_RETURN_BOOL(false);
				if (!GIST_LEAF(ent))
					PG_RETURN_BOOL(true);
				break;

			case INETSTRAT_GE:
			case INETSTRAT_GT:
				if (order < 0)
					PG_RETURN_BOOL(false);
				if (order > 0 || !GIST_LEAF(ent))
					PG_RETURN_BOOL(true);
				break;
		}
	}

	/* Remaining checks are only for leaves and basic comparison strategies. */
	Assert(GIST_LEAF(ent));

	/*
	 * Check 5: network bit count
	 *
	 * Bits are used on the basic comparison of the addresses. Whole
	 * addresses only compared if their network bits are the same.
	 * See backend/utils/adt/network.c:network_cmp_internal for
	 * the original comparison.
	 */
	switch (strategy)
	{
		case INETSTRAT_LT:
		case INETSTRAT_LE:
			if (ip_bits(orig) < ip_bits(query))
				PG_RETURN_BOOL(true);
			if (ip_bits(orig) > ip_bits(query))
				PG_RETURN_BOOL(false);
			break;

		case INETSTRAT_EQ:
			if (ip_bits(orig) != ip_bits(query))
				PG_RETURN_BOOL(false);
			break;

		case INETSTRAT_GE:
		case INETSTRAT_GT:
			if (ip_bits(orig) > ip_bits(query))
				PG_RETURN_BOOL(true);
			if (ip_bits(orig) < ip_bits(query))
				PG_RETURN_BOOL(false);
			break;
	}

	order = bitncmp(ip_addr(orig), ip_addr(query), ip_maxbits(orig));

	/*
	 * Check 6: whole address
	 *
	 * Whole address check would not be required for most of the
	 * strategies.
	 */
	switch (strategy)
	{
		case INETSTRAT_LT:
			PG_RETURN_BOOL(order < 0);

		case INETSTRAT_LE:
			PG_RETURN_BOOL(order <= 0);

		case INETSTRAT_EQ:
			PG_RETURN_BOOL(order == 0);

		case INETSTRAT_GE:
			PG_RETURN_BOOL(order >= 0);

		case INETSTRAT_GT:
			PG_RETURN_BOOL(order > 0);
	}

	elog(ERROR, "unknown strategy for inet GiST");
}

/*
 * The GiST union function
 *
 * The union of the networks is the network which contain all of them.
 * The main question to calculate the union is that they have how many
 * bits in common. After calculating the common bits, address of any of
 * them can be used as the union by discarding the host bits.
 */
Datum
inet_gist_union(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GISTENTRY	   *ent = entryvec->vector;
	int				family,
					bits;
	unsigned char  *addr;
	inet		   *tmp;
	OffsetNumber 	i,
					numranges = entryvec->n;

	/* Initilize variables using the first key. */
	tmp = DatumGetInetP(ent[0].key);
	family = ip_family(tmp);
	bits = ip_bits(tmp);
	addr = ip_addr(tmp);

	for (i = 1; i < numranges; i++)
	{
		tmp = DatumGetInetP(ent[i].key);

		/*
		 * Return a network with the special number 0 on the family field
		 * for addresses from different familes.
		 */
		if (ip_family(tmp) != family)
		{
			family = 0;
			bits = 0;
			break;
		}

		if (bits > ip_bits(tmp))
			bits = ip_bits(tmp);

		if (bits != 0)
			bits = bitncommon(addr, ip_addr(tmp), bits);
	}

	/* Make sure any unused bits are zeroed. */
	tmp = (inet *) palloc0(sizeof(inet));

	/* Initilize the union as inet. */
	ip_family(tmp) = family;
	ip_bits(tmp) = bits;

	/* Clone maximum bytes of the address. */
	if (bits != 0)
		memcpy(ip_addr(tmp), addr, (bits + 7) / 8);

	/* Clean the partial byte. */
	if (bits % 8 != 0)
		ip_addr(tmp)[bits / 8] &= ~(0xFF >> (bits % 8));

	SET_INET_VARSIZE(tmp);

	PG_RETURN_INET_P(tmp);
}

/*
 * The GiST compress function
 */
Datum
inet_gist_compress(PG_FUNCTION_ARGS)
{
	GISTENTRY	   *ent = (GISTENTRY *) PG_GETARG_POINTER(0);

	PG_RETURN_POINTER(ent);
}

/*
 * The GiST decompress function
 */
Datum
inet_gist_decompress(PG_FUNCTION_ARGS)
{
	GISTENTRY	   *ent = (GISTENTRY *) PG_GETARG_POINTER(0);

	PG_RETURN_POINTER(ent);
}

/*
 * The GiST page split penalty function
 *
 * Penalty is reverse of the common IP bits of the two addresses.
 * Values bigger than 1 are used when the common IP bits cannot
 * calculated.
 */
Datum
inet_gist_penalty(PG_FUNCTION_ARGS)
{
	GISTENTRY	   *origent = (GISTENTRY *) PG_GETARG_POINTER(0);
	GISTENTRY	   *newent = (GISTENTRY *) PG_GETARG_POINTER(1);
	float		   *penalty = (float *) PG_GETARG_POINTER(2);
	inet		   *orig = DatumGetInetP(origent->key),
				   *new = DatumGetInetP(newent->key);
	unsigned int	minbits,
					commonbits;

	if (ip_family(orig) == ip_family(new))
	{
		minbits = Min(ip_bits(orig), ip_bits(new));

		if (minbits > 0)
		{
			commonbits = bitncommon(ip_addr(orig), ip_addr(new), minbits);

			if (commonbits > 0)
				*penalty = ((float) 1) / commonbits;
			else
				*penalty = 2;
		}
		else
			*penalty = 3;
	}
	else
		*penalty = 4;

	PG_RETURN_POINTER(penalty);
}

/*
 * The GiST PickSplit method
 *
 * There are two ways to split. First one is to split by address families.
 * In this case, addresses of one first appeared family will be put to the
 * left bucket, addresses of the other family will be put to right bucket.
 * Only the root should contain addresses from different families, so only
 * the root should be split this way.
 *
 * The second and the important way is to split by the union of the keys.
 * Union of the keys calculated same way with the inet_gist_union function.
 * The first and the last biggest subnets created from the calculated
 * union. In this case addresses contained by the first subnet will be put
 * to the left bucket, address contained by the last subnet will be put to
 * the right bucket.
 */
Datum
inet_gist_picksplit(PG_FUNCTION_ARGS)
{
	GistEntryVector *entryvec = (GistEntryVector *) PG_GETARG_POINTER(0);
	GISTENTRY	   *ent = entryvec->vector;
	GIST_SPLITVEC  *splitvec = (GIST_SPLITVEC *) PG_GETARG_POINTER(1);
	int				minfamily,
					maxfamily,
					minbits,
					commonbits,
					real_index;
	unsigned char  *addr;
	inet		   *tmp,
				   *left_union,
				   *right_union;
	OffsetNumber 	maxoff,
					nbytes,
					i,
				   *left,
				   *right;
	GISTENTRY	  **raw_entryvec;

	maxoff = entryvec->n - 1;
	nbytes = (maxoff + 1) * sizeof(OffsetNumber);

	left = (OffsetNumber *) palloc(nbytes);
	right = (OffsetNumber *) palloc(nbytes);

	splitvec->spl_left = left;
	splitvec->spl_right = right;

	splitvec->spl_nleft = 0;
	splitvec->spl_nright = 0;

	/* Initialize the raw entry vector. */
	raw_entryvec = (GISTENTRY **) malloc(entryvec->n * sizeof(void *));
	for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		raw_entryvec[i] = &(entryvec->vector[i]);

	/* Initilize variables using the first key. */
	tmp = DatumGetInetP(ent[FirstOffsetNumber].key);
	minfamily = ip_family(tmp);
	maxfamily = minfamily;
	commonbits = ip_bits(tmp);
	minbits = commonbits;
	addr = ip_addr(tmp);

	/* Start comparing from the second one to find the common bit count. */
	for (i = OffsetNumberNext(FirstOffsetNumber); i <= maxoff;
			i = OffsetNumberNext(i))
	{
		real_index = raw_entryvec[i] - entryvec->vector;

		tmp = DatumGetInetP(entryvec->vector[real_index].key);
		Assert(tmp != NULL);

		/*
		 * If there are networks from different address families the split
		 * will be based on the family. So, first set the common bit count
		 * to 0. Then, update the minfamily and the maxfamily variables.
		 */
		if (ip_family(tmp) != minfamily && ip_family(tmp) != maxfamily)
		{
			commonbits = 0;

			if (ip_family(tmp) < minfamily)
				minfamily = ip_family(tmp);

			if (ip_family(tmp) > maxfamily)
				maxfamily = ip_family(tmp);
		}

		if (minbits > ip_bits(tmp))
			minbits = ip_bits(tmp);

		if (commonbits > ip_bits(tmp))
			commonbits = ip_bits(tmp);

		if (commonbits != 0)
			commonbits = bitncommon(addr, ip_addr(tmp), commonbits);
	}

	/* Make sure any unused bits are zeroed. */
	left_union = (inet *) palloc0(sizeof(inet));
	right_union = (inet *) palloc0(sizeof(inet));

	ip_family(left_union) = minfamily;
	ip_family(right_union) = maxfamily;

	if (minfamily != maxfamily)
	{
		Assert(minfamily < maxfamily);

		for (i = FirstOffsetNumber; i <= maxoff; i = OffsetNumberNext(i))
		{
			real_index = raw_entryvec[i] - entryvec->vector;
			tmp = DatumGetInetP(entryvec->vector[real_index].key);

			if (ip_family(tmp) != maxfamily)
			{
				if (ip_family(tmp) != minfamily)
					ip_family(left_union) = 0;

				left[splitvec->spl_nleft++] = real_index;
			}
			else
				right[splitvec->spl_nright++] = real_index;
		}
	}
	else
	{
		Assert(minfamily > 0);

		/*
		 * If all of the bits are common; there is no chance to split
		 * properly. It should mean that all of the elements have the same
		 * network address.
		 */
		if (commonbits != minbits)
			++commonbits;
		else
			ereport(DEBUG1,
					(errmsg("inet GiST cannot pict to split"),
					 errmsg_internal("all %d bits are the same for all items",
							commonbits)));

		ip_bits(left_union) = commonbits;
		ip_bits(right_union) = commonbits;

		/* Clone maximum bytes of the address to the left side. */
		memcpy(ip_addr(left_union), addr, (commonbits + 6) / 8);
		addr = ip_addr(left_union);

		/* Clean the partial byte on the left side. */
		if ((commonbits - 1) % 8 != 0)
			addr[(commonbits - 1) / 8] &= ~(0xFF >> ((commonbits - 1) % 8));

		/* Clone maximum bytes of the address to the right side. */
		memcpy(ip_addr(right_union), addr, (commonbits + 6) / 8);
		addr = ip_addr(right_union);

		/* Set the last network bit of the address for the one on the right side. */
		addr[(commonbits - 1) / 8] |= 1 << ((8 - (commonbits % 8)) % 8);

		for (i = FirstOffsetNumber; i < entryvec->n; i = OffsetNumberNext(i))
		{
			real_index = raw_entryvec[i] - entryvec->vector;
			tmp = DatumGetInetP(entryvec->vector[real_index].key);

			if (bitncmp(addr, ip_addr(tmp), commonbits) != 0)
				left[splitvec->spl_nleft++] = real_index;
			else
				right[splitvec->spl_nright++] = real_index;
		}
	}

	SET_INET_VARSIZE(left_union);
	SET_INET_VARSIZE(right_union);

	splitvec->spl_ldatum = InetPGetDatum(left_union);
	splitvec->spl_rdatum = InetPGetDatum(right_union);

	PG_RETURN_POINTER(splitvec);
}

/*
 * The GiST equality function
 */
Datum
inet_gist_same(PG_FUNCTION_ARGS)
{
	inet		   *left = PG_GETARG_INET_P(0);
	inet		   *right = PG_GETARG_INET_P(1);
	bool		   *result = (bool *) PG_GETARG_POINTER(2);

	*result = (ip_family(right) == ip_family(left) &&
			   ip_bits(right) == ip_bits(left) &&
			   bitncmp(ip_addr(left), ip_addr(right), ip_maxbits(left)) == 0);

	PG_RETURN_POINTER(result);
}

