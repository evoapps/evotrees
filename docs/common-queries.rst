
Number of generations (in a given timespan)::

    MATCH (extant:Revision) <-[generation:..]- (root:Revision)
    RETURN len(generation);

At junctions where Revisions point to one or more child Revisions,
what is the likelihood that the extant Revision is of higher quality
than the parent Revision and all other children Revisions?
