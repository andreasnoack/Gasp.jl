mutable struct Garray{T}
    ahandle::Ref{Ptr{Void}}
    access_iob::IOBuffer
    access_arr::Array
end

const GarrayMemoryHandle = IOBuffer

function free!(ga::Garray)
     ccall((:garray_destroy, libgasp), Void,
        (Ptr{Void},),
        ga.ahandle[])

    global num_garrays
    num_garrays -= 1
    exiting && num_garrays == 0 && __shutdown__()
end

function Garray(::Type{T}, num_elems::Int64) where T
    a = Garray{T}(Ref{Ptr{Void}}(), IOBuffer(), [])
    r = ccall((:garray_create, libgasp), Cint,
        (Ptr{Void}, Int64, Int64, Ptr{Int64}, Ptr{Void}),
        ghandle[1], num_elems, sizeof(T), C_NULL, a.ahandle)
    if r != 0
        error("construction failure")
    end
    global num_garrays
    num_garrays += 1
    finalizer(a, free!)
    return a
end

function length(ga::Garray)
    ccall((:garray_length, libgasp), Int64, (Ptr{Void},), ga.ahandle[])
end

function elemsize(ga::Garray)
    ccall((:garray_elemsize, libgasp), Int64, (Ptr{Void},), ga.ahandle[])
end

function get(ga::Garray{T}, lo::Int64, hi::Int64) where T
    adjlo = lo - 1
    adjhi = hi - 1
    getlen = hi - lo + 1
    cbuflen = getlen * sizeof(T)
    cbuf = Array{UInt8}(cbuflen)
    r = ccall((:garray_get, libgasp), Cint, (Ptr{Void}, Int64, Int64,
              Ptr{Void}), ga.ahandle[], adjlo, adjhi, cbuf)
    if r != 0
        error("Garray get failed")
    end
    iob = IOBuffer(cbuf)
    buf = Vector{T}(getlen)
    for i = 1:length(buf)
        try
            buf[i] = deserialize(iob)
        catch exc
            if !isa(exc, UndefRefError) && !isa(exc, BoundsError)
                rethrow()
            end
            # this is expected when an array element is uninitialized
        end
        seek(iob, i * sizeof(T))
    end
    return buf, iob
end

function put!(ga::Garray{T}, lo::Int64, hi::Int64, buf::Array{T}) where T
    adjlo = lo - 1
    adjhi = hi - 1
    putlen = hi - lo + 1
    cbuflen = putlen * sizeof(T)
    cbuf = Array{UInt8}(cbuflen)
    iob = IOBuffer(cbuf, true, true)
    # for i = 1:length(buf)
        # serialize(iob, buf[i])
        serialize(iob, buf)
        # seek(iob, i * ga.elem_size)
    # end
    r = ccall((:garray_put, libgasp), Cint, (Ptr{Void}, Int64, Int64,
              Ptr{Void}), ga.ahandle[], adjlo, adjhi, cbuf)
    if r != 0
        error("Garray put failed")
    end
end

function distribution(ga::Garray, rank::Int64)
    lo = Ref{Int64}(0)
    hi = Ref{Int64}(0)
    r = ccall((:garray_distribution, libgasp), Cint,
        (Ptr{Void}, Int64, Ptr{Int64}, Ptr{Int64}),
        ga.ahandle[], rank-1, lo, hi)
    if r != 0
        error("could not get distribution")
    end
    llo = lo[] + 1
    lhi = hi[] + 1
    return llo, lhi
end

function access(ga::Garray{T}, lo::Int64, hi::Int64) where T
    p = Ref{Ptr{Void}}()
    r = ccall((:garray_access, libgasp), Cint,
        (Ptr{Void}, Int64, Int64, Ptr{Ptr{Void}}),
        ga.ahandle[], lo-1, hi-1, p)
    if r != 0
        error("could not get access")
    end
    acclen = hi - lo + 1
    buf = Vector{T}(acclen)
    if length(buf) == 0
        return buf
    end
    cbuflen = acclen * sizeof(T)
    iob = IOBuffer(unsafe_wrap(Array, convert(Ptr{UInt8}, p[]), cbuflen),
                   true, true)

    for i = 1:length(buf)
        try
            buf[i] = deserialize(iob)
        catch exc
            if !isa(exc, UndefRefError) && !isa(exc, BoundsError)
                rethrow()
            end
            # this is expected when an array element is uninitialized
        end
        seek(iob, i * sizeof(T))
    end
    seek(iob, 0)
    ga.access_iob = iob
    ga.access_arr = buf
    return buf
end

function flush(ga::Garray{T}) where T
    if ga.access_arr != []
        for i = 1:length(ga.access_arr)
            try serialize(ga.access_iob, ga.access_arr[i])
            catch exc
                if !isa(exc, UndefRefError)
                    rethrow()
                end
            end
            seek(ga.access_iob, i * sizeof(T))
        end
        ga.access_arr = []
    end
    ccall((:garray_flush, libgasp), Void, (Ptr{Void},), ga.ahandle[])
end

