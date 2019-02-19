#= ========== LevelDB ==================
LevelDb specific code.

Each DB should export the symbols in KVS.jl
=#
#using LevelDB

global path_splice = ""

mutable struct KVSLevelDb <: AbstractKVS
    name::String     # Name of db
    handle           # Handle to db object
end
KVSLevelDb(name::String) = KVSLevelDb(name, levelDb_open(name))

function levelDb_open(name::String)
    db_root::String = getindex(ENV, "HOME")
    db_dir::String = db_root * "/kinant/" * path_splice * "/"
    if stat(db_dir).inode == 0
        Base.mkdir(db_dir)
    end
    handle = LevelDB.open_db(db_dir * name, true)
    handle
end

function kvs_path_append(snippet::String)
    global path_splice = snippet
end

function kvs_open(hdl::KVSLevelDb)
    hdl.handle = levelDb_open(hdl.name)
    hld
end

function kvs_close(hdl::KVSLevelDb)
    close_db(hdl.handle)
    hdl.name = ""
    hdl.handle = nothing
end

function kvs_put(hdl::KVSLevelDb, key, value)
    b = byte_array(value)
    LevelDB.db_put(hdl.handle, byte_array(key), b, sizeof(b))
end

function kvs_get(hdl::KVSLevelDb, key)
    r = LevelDB.db_get(hdl.handle, byte_array(key))
    return length(r) > 0 ? array_to_type(r) : nothing
end

"""
    kvs_get_many(db::KVSLevelDb, first, last, max::Int; ktype::Type=Any; vtype::Type=Any)
Return *max* entries beginning from *first* and upto but not including *last*.
Optionally specify the type of key and value.
"""
function kvs_get_many(db::KVSLevelDb, first, last, max::Integer; ktype::Type=Any, vtype::Type=Any)
    #first = "\u00000000000000000000000000000000"
    #last  = "\uffffffffffffffffffffffffffffffff"
    keys = Vector{ktype}(max)
    vals = Vector{vtype}(max)
    i::Int = 0
    for (k, v) in LevelDB.db_range(db.handle, byte_array(first), byte_array(last))
        if array_to_type(k) == last break end # Don't include last
        i += 1
        if i > max # Fill only max entries
            i -= 1
            break
        end
        keys[i] = array_to_type(k)
        vals[i] = array_to_type(v)
    end
    (keys, vals, i)
end

#=
function kvs_list_channels(db::KVSLevelDb)
    @assert db.name == "properties"
    first = "\u00000000000000000000000000000000"
    last  = "\uffffffffffffffffffffffffffffffff"
    keys = Vector{id_t}(0)
    for (k, v) in LevelDB.db_range(db.handle, first, last)
        key = array_to_type(k, id_t)
        push!(keys, key)
    end
    keys
end
=#

"""
Update the stream in the db atomically. For level Db this is just a
plain update as each process holds a lock on the db.
"""
function kvs_atomic_update(db::KVSLevelDb, cid, s, op::atomic_op)
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

    val = byte_array(s)
    LevelDB.db_put(db.handle, byte_array(cid), val, sizeof(val))
    s
end

"""
Try to atomically update db. For level Db this is just a
plain update as each process holds a lock on the db.
"""
function kvs_atomic_try_update(db::KVSLevelDb, cid, s)
    @assert db.name == "recent"
    val = byte_array(s)
    LevelDB.db_put(db.handle, byte_array(cid), val, sizeof(val))
    true
end

"""
Warning! Extreme caution! Deletes db!
"""
function kvs_clobber!(db::KVSLevelDb)
    db_root::String = getindex(ENV, "HOME")
    db_path = db_root * "/thimble/" * db.name
    kvs_close(db)
    run(`mkdir -p $db_root/bak`)
    run(`mv $db_path $db_root/bak/`)
    run(`unlink $db_path`)
end


"""
    db_chunk_exists(fp::fp_t)

Returns true if chunk exists in db.
"""
function chunk_exists(db::KVSLevelDb, fp)
    c = LevelDB.db_get(db.handle, byte_array(fp))
    if (length(c) == 0) return false else return true end
end

"""
return true if chunk already exists, else put chunk and return false
"""
function kvs_put_chunk(db::KVSLevelDb, input::Vector{UInt8}, fp, first::Int64, last::Int64)
    if chunk_exists(db, fp)
        return true
    else
        val = byte_array(input[first:last])
        LevelDB.db_put(db.handle, byte_array(fp), val, length(val))
    end
end

"""
Atomically increment the chunk ref counts for all chunks in array.
The incrementing of each chunk's ref is atomic.

For levelDb increment is implemented by a get and put.
"""
function kvs_atomic_batch_increment(db::KVSLevelDb, chunks)
    @assert db.name == "chunk_ref"
    saved::UInt64 = 0        # Bytes saved for this extent
    prev_offset::UInt64 = 0
    index::UInt32 = 0
    exists::BitVector =  falses(length(chunks))
    for i in chunks
        ref::UInt64 = 0
        index += 1
        ret = LevelDB.db_get(db.handle, byte_array(i.fp)) # Get ref
        if (length(ret) != 0)
            ref = array_to_type(ret, UInt64)
        end

        # Compute return values
        if (ref > 0)
            exists[index] = true
            saved += i.offset - prev_offset
        end
        prev_offset = i.offset

        # Increment ref and save
        ref = ref + 1
        val = byte_array(ref)
        LevelDB.db_put(db.handle, byte_array(i.fp), val, sizeof(val)) # Put ref
    end
    (exists, saved)
end


# helper functions
