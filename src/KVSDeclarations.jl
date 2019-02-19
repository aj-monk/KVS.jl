using DataStructures
using Serialization

const iobs = Stack(IOBuffer)

function get_iob()
    try
        return pop!(iobs)
    catch e
        return IOBuffer()
    end
end

function unget_iob(iob)
    push!(iobs, iob)
end

"""
    byte_array(x)

Serialize Julia type *x* such that it can be written to
a db or file
"""
function byte_array(x)
    iob = IOBuffer()
    serialize(iob, x)
    iob.data
end


struct KVSSerializeException <: Exception
    msg::String
end

"""
    array_to_type{T}(arr, ::Type{T})

Deserialize from Array{UInt8} back to Julia type.
"""
function array_to_type(arr, ::Type{T}) where T
    iob = IOBuffer(arr)
    seek(iob, 0)
    t = deserialize(iob)
    if ! isa(t, T)
        throw(KVSSerializeException("Could not deserialize to type"))
    end
    t
end

function array_to_type(arr)
    iob = IOBuffer(arr)
    seek(iob, 0)
    t = deserialize(iob)
    t
end
