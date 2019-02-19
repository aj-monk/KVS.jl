__precompile__()

module KVS
# Module Initializer
# function __init__()
# end

# Depending modules


# Exported
export KVSLevelDb, kvs_open, kvs_close, kvs_put, kvs_get, kvs_list_channels, kvs_delete, kvs_delete_range
export kvs_atomic_update, kvs_atomic_batch_increment, kvs_wait_for_updaters
export kvs_atomic_try_update, kvs_put_chunk, byte_array, array_to_type
export AbstractKVS, kvs_clobber!, kvs_path_append, kvs_get_many
export atomic_op, ATOMIC_NONE, ATOMIC_INC, ATOMIC_DEC
export KVSRocksDB, kvs_put_sync
export kvs_create_checkpoint, kvs_get_db_root, kvs_write_batch
export KVSRiak, KVSConsul, kvs_put_atomic, kvs_get_index

# Declarations
abstract type AbstractKVS end
@enum atomic_op ATOMIC_NONE ATOMIC_INC ATOMIC_DEC # Passed to atomic_update()


# Source Files
include("KVSDeclarations.jl")
include("KVSLevelDb.jl")
include("KVSRocksDB.jl")
include("KVSRiak.jl")
include("KVSConsul.jl")
# Deprecated

end # module KVS
