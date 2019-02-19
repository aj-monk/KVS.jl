#= ========== Riak ==================
Riak specific code. See Riak HTTP API:
http://docs.basho.com/riak/kv/2.2.3/developing/api/http/

To setup Riak:
http://basho.com/posts/technical/running-riak-in-docker/
=#
# using Requests

struct KVSRiak <: AbstractKVS
    name::String     # Name of db
end

function kvs_open(hdl::KVSRiak)
    return true
end

function kvs_close(hdl::KVSRiak)
    hdl.name = ""
end

struct KVSRiakException <: Exception
    status::Int
end

function kvs_put(hdl::KVSRiak, key, value)
    b = byte_array(value)
    r = Requests.put("http://127.0.0.1:8098/buckets/$(hdl.name)/keys/$(key)?returnbody=true";
                     data = b)
    r.status != 200 && throw(KVSRiakException(r.status))
    r
end

function kvs_put_sync(hdl::KVSRiak, key, value)
    # Nothing fancy
    kvs_put(hdl, key, value)
end

function kvs_delete(hdl::KVSRiak, key)
    r = Requests.delete("http://127.0.0.1:8098/buckets/$(hdl.name)/keys/$(key)")
    (r.status != 204 || r.status != 404) && throw(KVSRiakException(r.status))
    r
end

function kvs_get(hdl::KVSRiak, key)
    r = Requests.get("http://127.0.0.1:8098/buckets/$(hdl.name)/keys/$(key)")
    r.status != 200 && throw(KVSRiakException(r.status))
    array_to_type(r.data)
end


"""
    db_chunk_exists(fp)

Returns true if chunk exists in db.
"""
function chunk_exists(db::KVSRiak, fp)
    try
        c = kvs_get(db, fp)
        return true
    catch e
        return false
    end
end

"""
return true if chunk already exists, else put chunk and return false
"""
function kvs_put_chunk(db::KVSRiak, input::Vector{UInt8}, fp, first::Int64, last::Int64)
    if chunk_exists(db, fp)
        return true
    else
        kvs_put(db, fp, input[first:last])
    end
end
