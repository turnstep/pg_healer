/*-------------------------------------------------------------------------
 *
 * pg_healer.c
 *        Mutant healing powers, activate!
 *
 * Copyright (c) 2016 Greg Sabino Mullane
 *
 * IDENTIFICATION
 *        pg_healer.c/pg_healer.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"              /* Duh */
#include "miscadmin.h"             /* if (superuser) */
#include "utils/rel.h"             /* lots! */
#include "utils/builtins.h"        /* text_to_cstring */
#include <sys/file.h>              /* flock */
#include "utils/relfilenodemap.h"  /* RelidByRelfilenode */
#include "access/heapam.h"
#include "storage/bufmgr.h"
#include "storage/smgr.h"
#include "storage/checksum.h"
#include "storage/checksum_impl.h"

PG_MODULE_MAGIC;

void _PG_init(void);
void _PG_fini(void);

static emit_log_hook_type prev_log_hook = NULL;

uint16 pg_healer_checksum(char *page, BlockNumber blockNumber);

static void pg_healer_error_hook(ErrorData *edata);

PG_FUNCTION_INFO_V1(pg_healer_corrupt);

PG_FUNCTION_INFO_V1(pg_healer_corrupt2);

PG_FUNCTION_INFO_V1(pg_healer_remove_from_buffer);

static void pg_healer_repair_buffer(char *pageBuffer, BlockNumber blockNumber, uint16 relType);

uint16 pg_healer_repair_external(BlockNumber blockNumber, char *pageBuffer, char *filepath);

static void pg_healer_repair_path(char *filepath, BlockNumber blockNumber);





/*
  Given a page and its block number, return a 16-bit checksum
*/

uint16
pg_healer_checksum(char *page, BlockNumber blockNumber)
{

    PageHeader    phdr = (PageHeader) page;
    uint16        header_checksum;
    uint32        actual_checksum;

    /* 
       The checksum itself must be zeroed out before computing a checksum
       on the page, so we squirrel the old value away
    */
    header_checksum = phdr->pd_checksum;
    phdr->pd_checksum = 0;

    /* Calculate the 32-bit checksum */
    actual_checksum = pg_checksum_block(page, BLCKSZ);

    /* Put the original checksum back into place */
    phdr->pd_checksum = header_checksum;

    /* The block number is added in as an additional check */
    actual_checksum ^= blockNumber;

    /* Sadly, we can only store 16-bit checksums. Add a 1 to prevent zeroes */
    return (actual_checksum % 65535) + 1;

}


/*
 * pg_healer_error_hook
 * Parse error messages to look for corruption issues
 */
static void
pg_healer_error_hook(ErrorData *edata)
{

    char         *p;
    BlockNumber  blockNumber;

    /* Example: ERROR:  invalid page in block 0 of relation base/174700/357157 */
    if (0 == strncmp(edata->message, "invalid page ", 13)
        && 0==strncmp(unpack_sql_state(edata->sqlerrcode), "XX001", 5))
    {
        /* Grab the block number */
        p = strstr(edata->message, "block ");
        if (NULL == p)
		{
            elog(INFO, "Could not determine the block number, aborting");
            return;
		}
		blockNumber = strtol(p, NULL, 10);

        /* Grab the relation path */
        p = strstr(edata->message, "relation");
        if (NULL == p)
		{
            elog(INFO, "Could not determine the relation path, aborting");
            return;
        }
        p = strchr(p, ' ');
        p++;
        pg_healer_repair_path(p, blockNumber);

		// Be nice and report the table name?
		// heaprel = RelidByRelfilenode(x, y);
    }

    /* Continue chain to previous hook (which may well report on an already fixed error!) */
    if (prev_log_hook)
        (*prev_log_hook) (edata);

} /* end of pg_healer_error_hook */



/*
  Purposefully corrupt a relation.
  Use with care!
  Arguments: relation name, corruption type, block number (optional)
*/

Datum
pg_healer_corrupt(PG_FUNCTION_ARGS)
{

    Relation    rel;
    ForkNumber  forkNumber = 0; /* only worry about main fork for now */
    int64       blockNumber = 0;
    char        pageBuffer[BLCKSZ];
    PageHeader  phdr;
    char        *action = NULL;
    int         i;

    /*
      Maybe it should be stricter than superuser! :)
    */
    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be superuser to use raw functions"))));

    /* First argument is a relation: attempt to find it */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("relation cannot be null")));

    rel = relation_open(PG_GETARG_OID(0), AccessShareLock);

    RelationOpenSmgr(rel);

    /* If the table is empty, then there is no corruption to be done! */
    if (0 == RelationGetNumberOfBlocksInFork(rel, forkNumber)) {
        ereport(ERROR,
                (errcode(ERRCODE_WRONG_OBJECT_TYPE),
                 errmsg("cannot corrupt an empty table")));
        return 1;
    }

    /* User can supply a block number, or we default to zero */
    if (!PG_ARGISNULL(2)) {
        blockNumber = PG_GETARG_INT32(2);
    }

    /* Read in the page, and store the header */
    smgrread(rel->rd_smgr, forkNumber, blockNumber, pageBuffer);
    phdr = (PageHeader) pageBuffer;



    /* Do the corruption, based on the second argument */
    action = text_to_cstring(PG_GETARG_TEXT_PP(1));

    /*
      "freespace"
      Put non-zero bytes at start and end of freespace
    */
    if (strcmp(action, "freespace") == 0)
	{
        *(pageBuffer + phdr->pd_lower) = 87;
        *(pageBuffer + phdr->pd_upper-1) = 88;
        ereport(INFO, (errmsg("Free space corruption introduced at bytes %d and %d",
                              phdr->pd_lower, phdr->pd_upper-1),
                       errtable(rel)
					));
    }
    /*
      "pd_lsn"
      Mess up the pd_lsn in the header
    */
    else if (strcmp(action, "pd_lsn") == 0)
	{
        for (i=0; i < 8; i++)
		{
            *(pageBuffer + i) = 42 + i;
        }
        ereport(INFO, (errmsg("LSN corruption introduced at first 8 bytes"),
                       errtable(rel)
					));
    }
    /*
      "pd_special"
      Mess up the pd_special in the header
    */
    else if (strcmp(action, "pd_special") == 0)
	{
        *(pageBuffer + 16) = 3;
        ereport(INFO, (errmsg("pd_special now points to the wrong location"),
                       errtable(rel)
					));
    }
    
    /*
      "pd_pagesize_version"
      Mess up the pd_pagesize_version in the header
    */
    else if (strcmp(action, "pd_pagesize_version") == 0)
	{
        *(pageBuffer + 18) = (123 | 2);
        ereport(INFO, (errmsg("pd_pagesize_version is now quite incorrect"),
                       errtable(rel)
					));
    }

    else if (strcmp(action, "badrow") == 0)
	{
		for (i=5; i <= 10; i++)
		{
			*(pageBuffer + (BLCKSZ - i)) = 12 + i;
		}
		ereport(INFO, (errmsg("First tuple on page is now corrupted"),
                       errtable(rel)
					));
    }

    else
	{
        ereport(ERROR, (errmsg("Unknown corruption type \"%s\"", action)));
    }


    /* Write the changed page back to disk */
    smgrwrite(rel->rd_smgr, forkNumber, blockNumber, pageBuffer, false);

    /* Close and release lock */
    relation_close(rel, AccessShareLock);

    /* Force it out of shared buffers */
	DropRelFileNodeBuffers(rel->rd_smgr->smgr_rnode, 0, 0);

    return 0;

}


/*
  Remove a table from shared buffers
  Arguments: relation name
*/

Datum
pg_healer_remove_from_buffer(PG_FUNCTION_ARGS)
{

    Relation  rel;

    if (!superuser())
        ereport(ERROR,
                (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
                 (errmsg("must be superuser to use raw functions"))));

    /* First argument is a relation: attempt to find it */
    if (PG_ARGISNULL(0))
        ereport(ERROR,
                (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                 errmsg("relation cannot be null")));

    rel = relation_open(PG_GETARG_OID(0), AccessShareLock);

    RelationOpenSmgr(rel);

    /* Close and release lock */
    relation_close(rel, AccessShareLock);

    /* Force it out of shared buffers */
	DropRelFileNodeBuffers(rel->rd_smgr->smgr_rnode, 0, 0);

    return 0;

}


/*
  pg_healer_repair_buffer - attempt to modify in place to fix errors

  The caller usually has an open file handle, so always return
 */
static void
pg_healer_repair_buffer(char *pageBuffer, BlockNumber blockNumber, uint16 relType)
{

    uint16  actual_checksum, header_checksum;
    uint16  pd_lower, pd_upper;
    int     i;
    bool    problems_found = 0, problems_fixed = 0;
    uint16  pd_special, pd_pagesize_version;
    int     header_size, header_version;
    int     good_version = 4; /* TODO: Adjust to < 4 for really old versions of postgres */

    elog(DEBUG1, "Starting pg_healer_repair_buffer, blockNumber is %u", blockNumber);

    /* Grab both the reported and actual checksums */
    memcpy(&header_checksum, (pageBuffer+8), 2);
    actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);

    /* For now, simply bail if the checksums match up */
    if (header_checksum == actual_checksum)
	{
        elog(INFO, "Checksums already match!");
        return;
    }

    /* Check 1: free space must be all zeroes */
    memcpy(&pd_lower, (pageBuffer+12), 2);
    memcpy(&pd_upper, (pageBuffer+14), 2);
    for (i=pd_lower; i < pd_upper; i++)
	{
        if (0 != *(pageBuffer+i))
		{
            problems_found++;
            elog(DEBUG1, "Found a non-zero value in free space portion of page: at byte %d, saw %d", i, *(pageBuffer+i));
            *(pageBuffer+i) = 0;
            problems_fixed++;
        }
    }

    /* Check 2: page size and version */
    memcpy(&pd_pagesize_version, (pageBuffer+18), 2);
    header_size = pd_pagesize_version & 0xFF00;
    header_version = pd_pagesize_version & 0x00FF;
    i = 0;
    if (BLCKSZ != header_size)
	{
        elog(INFO, "Found invalid page size in header: %d. Should be %d", header_size, BLCKSZ);
        i++;
    }
    if (good_version != header_version)
	{
        elog(INFO, "Found invalid version in header: %d. Should be %d", header_version, good_version);
        i++;
    }
    if (i)
	{
        problems_found += i;
        *(pageBuffer+18) = BLCKSZ | good_version;
        problems_fixed += i;
    }

	/* Check 3: pd_special */
    memcpy(&pd_special, (pageBuffer+16), 2);
	if (1 == relType) {
		if (BLCKSZ != pd_special) {
			elog(INFO, "Found invalid pd_special: must be page size (%d) for tables, not %d", BLCKSZ, pd_special);
			problems_found++;
			*(pageBuffer+16) = 0;
			*(pageBuffer+17) = 0;
			problems_fixed ++;
		}
	}

    elog(DEBUG1, "Problems found: %d  Problems fixed: %d", problems_found, problems_fixed);

    /* If we fixed all the problems, see if the checksums now match */
    if (problems_found && problems_found == problems_fixed)
	{
        actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);
        if (actual_checksum == header_checksum)
		{
            elog(DEBUG1, "All problems have been fixed!");
        }
    }

    return;
}


/*
  pg_healer__repair_external - attempt to repair using external resources
 */
uint16
pg_healer_repair_external(BlockNumber blockNumber, char *pageBuffer, char *filePath)
{

    const char  *healerdir = "pg_healer";
    char        *newpath = NULL;
    char        ext_pageBuffer[BLCKSZ];
    FILE        *ext_fh;
    uint16      header_checksum, actual_checksum;
    uint16      ext_header_checksum, ext_actual_checksum;
    uint16      pd_lower, pd_upper, ext_pd_lower, ext_pd_upper;
    int         i, j;
	int         problems_found = 0, problems_fixed = 0;
	uint16      byte_diffs, row_diffs;
	ItemId	    itemId;
	uint32      offset, flags, length;
	uint32      ext_offset, ext_flags, ext_length;


    elog(DEBUG1, "Starting pg_healer_repair_external, blockNumber is %u", blockNumber);

    /* Build the path to our external-to-the-datadir copy */
    newpath = palloc(strlen(healerdir) + strlen(filePath) + 1);
    sprintf(newpath, "%s/%s", healerdir, filePath);

    /* Attempt to open the external file */
    ext_fh = fopen(newpath, "rb");
    pfree(newpath);
    if (NULL == ext_fh)
	{
        elog(INFO, "Could not find an external file to aid in repairs");
        return 0;
    }

    /* Read the external page into memory */
    fseek(ext_fh, (BLCKSZ * blockNumber), SEEK_SET);
    fread(ext_pageBuffer, BLCKSZ, 1, ext_fh);
    fclose(ext_fh);

    /* Get both reported and actual checksums for both pages */
    memcpy(&header_checksum, (pageBuffer+8), 2);
    memcpy(&ext_header_checksum, (ext_pageBuffer+8), 2);
    actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);
    ext_actual_checksum = pg_healer_checksum(ext_pageBuffer, blockNumber);
    elog(DEBUG1, "(%s) Checksums: header=%d ext_header=%d  actual=%d ext_actual=%d",
         "external", header_checksum, ext_header_checksum, actual_checksum, ext_actual_checksum);

    /* If the checksums match, we assume we can simply copy the whole page over */
    if (header_checksum == ext_header_checksum)
	{
        for (i=0; i< BLCKSZ; i++)
		{
            if (pageBuffer[i] != ext_pageBuffer[i])
			{
                elog(DEBUG2, "Copy position %d", i);
                problems_found++;
                pageBuffer[i] = ext_pageBuffer[i];
                problems_fixed++;
            }
        }
        return 2;
    }

    /* We need some information for help traversing below */
    memcpy(&pd_lower, (pageBuffer+12), 2);
    memcpy(&ext_pd_lower, (ext_pageBuffer+12), 2);
    memcpy(&pd_upper, (pageBuffer+14), 2);
    memcpy(&ext_pd_upper, (ext_pageBuffer+14), 2);

    /* 
       Otherwise, we have to be a little more deliberate. We want to try the most obvious 
       solutions first, and keep checking the checksum along the way.
    */

	/*
	  First, copy over row by row, to see if the corruption is inside the rows.
	  We only copy rows that exist on both sides, obviously.
	  There are much more brains to be added here, but this is a pretty good start:
	  it should catch many single-bit errors, assuming the external source is fairly up to date.
	*/
	row_diffs = 0;
	for (i=24; i < ext_pd_lower; i+=4)
	{
		if (i > pd_lower) /* Ran out of tuples, time to leave */
		{
			break;
		}
		//elog(INFO, "Looking for pointer at byte %d, pd_lower is %d", i, ext_pd_lower);
		itemId = ( (ItemId) (pageBuffer+i));
		offset = ItemIdGetOffset(itemId);
		flags = ItemIdGetFlags(itemId);
		length = ItemIdGetLength(itemId);

		//memcpy(itemId, (ext_pageBuffer+i), 4);
		itemId = ( (ItemId) (ext_pageBuffer+i));
		ext_offset = ItemIdGetOffset(itemId);
		ext_flags = ItemIdGetFlags(itemId);
		ext_length = ItemIdGetLength(itemId);

		elog(DEBUG2, "Current offset/flag/length: %d / %d / %d  External: %d / %d / %d\n",
			 offset, flags, length,
			 ext_offset, ext_flags, ext_length);

		/* Lengths must be equal or things get too dicey */
		if (offset != ext_offset || length != ext_length) {
			elog(DEBUG1, "Cowardly failing to compare rows");
			continue;
		}

		/* Scan each byte in this tuple */
		byte_diffs = 0;
		for (j=offset; j < offset + length; j++)
		{
			if (pageBuffer[j] != ext_pageBuffer[j])
			{
				elog(DEBUG2, "Copy position %d", j);
				byte_diffs++;
				pageBuffer[j] = ext_pageBuffer[j];
			}
		}

		if (byte_diffs)
		{
			row_diffs++;
			/* Time to give it a try */
			actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);
			elog(DEBUG1, "(%s) Checksums: header=%d ext_header=%d  actual=%d ext_actual=%d",
				 "external", header_checksum, ext_header_checksum, actual_checksum, ext_actual_checksum);
			if (actual_checksum == header_checksum)
			{
				elog(DEBUG1, "Fixed with data section");
				return 3;
			}
		}

	}

    return 0;

} /* end of pg_healer_repair_external */



static void
pg_healer_repair_path(char *filePath, BlockNumber blockNumber)
{

    FILE    *fh;
    uint16  actual_checksum = 0, header_checksum = 0;
    char    pageBuffer[BLCKSZ];
	char    repair_type = 0;

    elog(DEBUG1, "Starting pg_healer_repair_path, filePath is \"%s\", blockNumber is %u", filePath, blockNumber);

    /* Open the file manually */
    fh = fopen(filePath, "r+b");
    if (NULL==fh)
	{
        elog(WARNING, "Failed to open file \"%s\"", filePath);
        return;
    }

    /* Lock, then find the page we are looking for */
    flock( fileno(fh), LOCK_EX); /* Should we LOCK_NB? */
    if (fseek(fh, (BLCKSZ * blockNumber), SEEK_SET) < 0)
	{
        elog(INFO, "Failed to seek to position %u in file \"%s\"", (BLCKSZ * blockNumber), filePath);
        fclose(fh);
        return;
    }

    /* Read it into memory, but leave the file open */
    fread(pageBuffer, BLCKSZ, 1, fh);

    /* Grab what this page thinks the checksum should be, and what it actually is */
    memcpy(&header_checksum, &pageBuffer[8], 2);
    actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);

	/* Sanity check: in the case the caller is in error */
	if (actual_checksum == header_checksum) {
		elog(NOTICE, "pg_healer_repair_path was calledm but the checksums match!");
		return;
	}

    /* First pass at repair; the "1" indicates this is a normal table */
	repair_type = 1;
    pg_healer_repair_buffer(pageBuffer, blockNumber, 1);

    /* Regenerate the checksum to see if the repairs worked */
    actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);

    /* Second pass at repairs, if checksums still do not match */
    if (actual_checksum != header_checksum) {

		repair_type = pg_healer_repair_external(blockNumber, pageBuffer, filePath);

        /* As before, check for matching checksums */
        actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);

        /* Third pass at repairs: user intervention */
        if (actual_checksum != header_checksum) 
        {
			repair_type = 4;
        }

        /* Final checksum */
        actual_checksum = pg_healer_checksum(pageBuffer, blockNumber);

        /* Time to throw in the towel */
        if (actual_checksum != header_checksum)
		{
            elog(INFO, "pg_healer was unable to repair the relation at \"%s\"", filePath);
            fclose(fh);
            return;
        }
    }

    /* Checksums now match, so we can rewrite the page into the file */
    if (fseek(fh, (BLCKSZ * blockNumber), SEEK_SET) < 0)
	{
        elog(INFO, "Failed to seek to position %u in file \"%s\"", (BLCKSZ * blockNumber), filePath);
        fclose(fh);
        return;
    }

    /* Dump our modified page to the file */
    fwrite(pageBuffer, 1, BLCKSZ, fh);

    /* Close and unlock the file */
    fclose(fh);

    elog(INFO, "File has been healed: %s (%s)",
		 filePath,
		 1==repair_type ? "intrinsic healing" :
		 2==repair_type ? "external checksum match" :
		 3==repair_type ? "external tuple healing" :
		 "unknown repair type"
		);

    return;

} /* end of pg_healer_repair_path */




/*
 * _PG_init
 * Entry point loading hooks
 */
void
_PG_init(void)
{
    prev_log_hook = emit_log_hook;
    emit_log_hook = pg_healer_error_hook;
}

/*
 * _PG_fini
 * Exit point unloading hooks
 */
void
_PG_fini(void)
{
    emit_log_hook = prev_log_hook;
}
