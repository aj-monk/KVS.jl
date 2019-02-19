#= ========== Consul KVS ==================
Consul KVS specific code.
For the API refer https://www.consul.io/api/kv.html
=#
# using Requests

struct KVSConsul <: AbstractKVS
    name::String     # Name of db
end

function kvs_close(hdl::KVSConsul)
    return true
end

struct KVSConsulException <: Exception
    status::Int
end

struct KVSConsulCASExpired <: Exception
    tries::Int
end

function kvs_put(hdl::KVSConsul, key, value)
    k = hdl.name * base64encode(byte_array(key))
    v = byte_array(value)
    r = Requests.put("http://127.0.0.1:8500/v1/kv/$(k)"; data = v)
    (r.status != 200 || Requests.json(r)[1] == false) && throw(KVSConsulException(r.status))
    r
end

function kvs_put_atomic(hdl::KVSConsul, key, value, index)
    k = hdl.name * base64encode(byte_array(key))
    v = byte_array(value)
    r = Requests.put("http://127.0.0.1:8500/v1/kv/$(k)?cas=$(index)"; data = v)
    r.status != 200 && throw(KVSConsulException(r.status))
    Requests.json(r)[1]
end

function kvs_put_sync(hdl::KVSConsul, key, value)
    # Nothing fancy
    kvs_put(hdl, key, value)
end

function kvs_delete(hdl::KVSConsul, key)
    k = hdl.name * base64encode(byte_array(key))
    r = Requests.delete("http://127.0.0.1:8500/v1/kv/$(k)")
    r.status != 200 && return false
    true
end

function kvs_get(hdl::KVSConsul, key)
    k = hdl.name * base64encode(byte_array(key))
    r = Requests.get("http://127.0.0.1:8500/v1/kv/$(k)?raw")
    r.status != 200 && return nothing
    array_to_type(r.data)
end

"""
    kvs_get_index(hdl::KVSConsul, key)
For the given key, returns (value, index), where index is an integer
that is used to atomically update value.
"""
function kvs_get_index(hdl::KVSConsul, key)
    k = hdl.name * base64encode(byte_array(key))
    r = Requests.get("http://127.0.0.1:8500/v1/kv/$(k)")
    r.status != 200 && return (nothing, 0)
    j = Requests.json(r)[1]
    (array_to_type(base64decode(j["Value"])), j["ModifyIndex"])
end

function kvs_get_many(hdl::KVSConsul, T::Type)
    prefix = hdl.name
    r = Requests.get("http://127.0.0.1:8500/v1/kv/$(prefix)?recurse")
    r.status != 200 && return nothing
    jsn = Requests.json(r)
    keys = Vector{Any}()
    for i in jsn
        #println("$i $(base64decode(i[length(hdl.name)+1:length(i)]))")
        k = array_to_type(base64decode(i["Value"]))[1]
        push!(keys, k)
    end
    keys
end

using Test
function runtests_consul()
    @testset "Consul tests:" begin
        db = KVSConsul("mydb")
        @test db != nothing
        k = rand(UInt128)
        v = rand(UInt8, 64)
        @test kvs_put(db, k, v) != nothing
        ret = kvs_get(db, k)
        @test ret != nothing
        @test ret == v
        (ret, idx) = kvs_get_index(db, k)
        @test ret != nothing
        @test ret == v
        v1 = rand(UInt8, 64)
        @test kvs_put_atomic(db, k, v1, idx-1) == false
        @test kvs_put_atomic(db, k, v1, idx+1) == false
        @test kvs_put_atomic(db, k, v1, idx) == true
        (ret, idx1) = kvs_get_index(db, k)
        @test ret == v1
        @test idx1 != idx
    end
end
