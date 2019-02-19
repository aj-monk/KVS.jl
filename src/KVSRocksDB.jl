#= ========== RocksDB ==================
RocksDb specific code.

Each DB should export the symbols in KVS.jl
=#
using RocksDB

struct KVSRocksDB <: AbstractKVS
    name::String     # Name of db
    handle           # Handle to db object
end
KVSRocksDB(name::String) = KVSRocksDB(name, rocksDB_open(name))

KVSRocksDB(name::String, path::String) = KVSRocksDB(name, rocksDB_open(name, path))

function rocksDB_open(name::String, db_dir::String)
    if stat(db_dir).inode == 0
        Base.mkdir(db_dir)
    end
    handle = RocksDB.open_db(db_dir * name, true)
    handle
end

rocksDB_open(name::String) = rocksDB_open(name, kvs_get_db_root())

function kvs_get_db_root()
    getindex(ENV, "HOME") * "/kinant/"
end

function kvs_get_db_root(fs_id)
    getindex(ENV, "HOME") * "/kinant/" * hex(fs_id, sizeof(fs_id))
end

function kvs_open(hdl::KVSRocksDB)
    hdl.handle = rocksDB_open(hdl.name)
    hld
end

function kvs_close(hdl::KVSRocksDB)
    RocksDB.close_db(hdl.handle)
    hdl.name = ""
    hdl.handle = nothing
end

function kvs_put(hdl::KVSRocksDB, key, value)
    RocksDB.db_put(hdl.handle, key, value)
end

function kvs_put_sync(hdl::KVSRocksDB, key, value)
    RocksDB.db_put_sync(hdl.handle, key, value)
end

function kvs_delete(hdl::KVSRocksDB, key)
    RocksDB.db_delete(hdl.handle, key)
end

function kvs_delete_range(hdl::KVSRocksDB, first, last)
    RocksDB.db_delete_range(hdl.handle, first, last)
end

function kvs_get(hdl::KVSRocksDB, key)
    RocksDB.db_get(hdl.handle, key)
end

"""
    kvs_get_many(db::KVSRocksDB, first, last, max::Int; inc_first=false)
Return *max* entries beginning from *first* and upto but not including *last*.
Optionally specify whether the first entry needs to be included.
"""
function kvs_get_many(db::KVSRocksDB, first, last, max::Integer; inc_first=true, raw_read=false)
    #first = "\u00000000000000000000000000000000"
    #last  = "\uffffffffffffffffffffffffffffffff"
    @info "db: $(db.name) $(first) $(last) $(inc_first) $(raw_read)"
    keys = Vector{Any}(undef, max)
    vals = Vector{Any}(undef, max)
    i::Int = 0
    for (k, v) in RocksDB.db_range(db.handle, first, last; raw=raw_read)
        #if k == last break end # Don't include last
        !inc_first && (k == first) && continue # Don't include first
        i += 1
        if i > max # Fill only max entries
            i -= 1
            break
        end
        keys[i] = k
        vals[i] = v
    end
    (keys, vals, i)
end

function kvs_write_batch(db::KVSRocksDB, keys, vals; raw_write=false)
    batch = RocksDB.create_write_batch()
    for i = 1:length(keys)
        RocksDB.batch_put(batch, keys[i], vals[i]; raw=raw_write)
    end
    RocksDB.write_batch(db.handle, batch)
end

function kvs_create_checkpoint(db::KVSRocksDB, path::String)
    RocksDB.db_create_checkpoint(db.handle, path)
end

"""
Update the stream in the db atomically. For Rocks Db this is just a
plain update as each process holds a lock on the db.
"""
function kvs_atomic_update(db::KVSRocksDB, cid, s, op::atomic_op)
    @assert db.name == "recent"
    if op == ATOMIC_INC
        # if s.state != S_OPEN throw(StreamStateException("State != S_OPEN")) end
        s.ref = s.ref + 1
        s.seq = s.seq + 1
    elseif op == ATOMIC_DEC
        s.ref = s.ref - 1
    end
    # TODO: for consul, need to check if someone else is commiting (S_COMMITTING)
    # the stream. Then this process should back off. The best way to do that
    # would be to have a state transition checker throw an exception if the
    # transition is not right.

    RocksDB.db_put(db.handle, cid, s)
    s
end

"""
Try to atomically update db. For Rocks Db this is just a
plain update as each process holds a lock on the db.
"""
function kvs_atomic_try_update(db::KVSRocksDB, cid, s)
    @assert db.name == "recent"
    RocksDB.db_put(db.handle, cid, s)
    true
end

"""
Warning! Extreme caution! Deletes db!
"""
function kvs_clobber!(db::KVSRocksDB)
    db_root::String = getindex(ENV, "HOME")
    db_path = db_root * "/thimble/" * db.name
    kvs_close(db)
    run(`mkdir -p $db_root/bak`)
    run(`mv $db_path $db_root/bak/`)
    run(`unlink $db_path`)
end


"""
    db_chunk_exists(fp)

Returns true if chunk exists in db.
"""
function chunk_exists(db::KVSRocksDB, fp)
    c = RocksDB.db_get(db.handle, fp)
    c == nothing && return false
    return true
end

"""
return true if chunk already exists, else put chunk and return false
"""
function kvs_put_chunk(db::KVSRocksDB, input::Vector{UInt8}, fp, first::Int64, last::Int64)
    if chunk_exists(db, fp)
        return true
    else
        RocksDB.db_put(db.handle, fp, input[first:last])
    end
end

# Dummy function on a single node system
function kvs_put_atomic(db::KVSRocksDB, key, value, index)
    kvs_put(db, key, value) # Ignore index
    true
end

# Dummy function on a single node system
function kvs_get_index(db::KVSRocksDB, key)
    v = kvs_get(db, key)
    return (v, rand(Int64)) # Return a random index
end

"""
Atomically increment the chunk ref counts for all chunks in array.
The incrementing of each chunk's ref is atomic.

For RocksDB increment is implemented by a get and put.
"""
function kvs_atomic_batch_increment(db::KVSRocksDB, chunks)
    @assert db.name == "chunk_ref"
    saved::UInt64 = 0        # Bytes saved for this extent
    index::UInt32 = 0
    exists::BitVector =  falses(length(chunks))
    for i in chunks
        ref::UInt64 = 0
        index += 1
        ret = RocksDB.db_get(db.handle, i.cksum) # Get ref
        (ret != nothing) && (ref = ret)

        # Compute return values
        if (ref > 0)
            exists[index] = true
            saved += i.range.stop - i.range.start
        end

        # Increment ref and save
        ref = ref + 1
        RocksDB.db_put(db.handle, i.cksum, ref) # Put ref
    end
    (exists, saved)
end

# helper functions
