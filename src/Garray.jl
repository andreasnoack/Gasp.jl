mutable struct Garray{T}
    ahandle::Ref{Ptr{Void}}
    access_arr::Vector{T}
end

function free!(ga::Garray)
     ccall((:garray_destroy, libgasp), Void,
        (Ptr{Void},),
        ga.ahandle[])
     return nothing
end

function Garray(::Type{T}, num_elems::Int64) where T
    @assert isbits(T)
    a = Garray{T}(Ref{Ptr{Void}}(), T[])
    r = ccall((:garray_create, libgasp), Cint,
        (Ptr{Void}, Int64, Int64, Ptr{Int64}, Ptr{Void}),
        ghandle[], num_elems, sizeof(T), C_NULL, a.ahandle)
    if r != 0
        error("construction failure")
    end
    finalizer(a, free!)
    return a
end

function length(ga::Garray)
    ccall((:garray_length, libgasp), Int64, (Ptr{Void},), ga.ahandle[])
end

function get(ga::Garray{T}, lo::Int64, hi::Int64; buffer = Vector{T}(hi - lo + 1)) where T
    adjlo = lo - 1
    adjhi = hi - 1
    getlen = hi - lo + 1
    r = ccall((:garray_get, libgasp), Cint,
        (Ptr{Void}, Int64, Int64, Ptr{Void}),
        ga.ahandle[], adjlo, adjhi, buffer)
    if r != 0
        error("Garray get failed")
    end
    return buffer
end

function put!(ga::Garray{T}, lo::Int64, hi::Int64, buf::Array{T}) where T
    adjlo = lo - 1
    adjhi = hi - 1
    putlen = hi - lo + 1
    @assert length(buf) == putlen
    r = ccall((:garray_put, libgasp), Cint, (Ptr{Void}, Int64, Int64,
              Ptr{Void}), ga.ahandle[], adjlo, adjhi, buf)
    if r != 0
        error("Garray put failed")
    end
end

function distribution(ga::Garray, rank::Int64)
    lo = Ref{Int64}(0)
    hi = Ref{Int64}(0)
    r = ccall((:garray_distribution, libgasp), Cint,
        (Ptr{Void}, Int64, Ptr{Int64}, Ptr{Int64}),
        ga.ahandle[], rank - 1, lo, hi)
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
        ga.ahandle[], lo - 1, hi - 1, p)
    if r != 0
        error("could not get access")
    end
    acclen = hi - lo + 1
    ga.access_arr = unsafe_wrap(Array, convert(Ptr{T}, p[]), acclen)

    return ga.access_arr
end
access(ga::Garray) = access(ga, distribution(ga, grank())...)

function flush(ga::Garray{T}) where T
    ccall((:garray_flush, libgasp), Void, (Ptr{Void},), ga.ahandle[])
    ga.access_arr = T[]
    return nothing
end

