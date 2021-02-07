module BatchSetup

export setup_batch, setup_sh, BatchInfo, SubmissionArguments


const SOURCEDIR = "\$HOME/Working-Repo/xml"
const DEFAULT_DATA = "8G"
const DEFAULT_TIME = "300:00:00"
const EMAIL = "gabriel.w.hassler@gmail.com"

mutable struct BatchInfo
    name::String
    filenames::Vector{String}
    multicore::Bool
    data::String
    run_time::String
    beast_dir::String
    dest_dir::String
    save::Bool
    save_files::Vector{String}
    save_every::Int
    load::Bool
    load_dir::String
    load_files::Vector{String}
    email::Bool
    tmp_dir::Bool
    source_dir::String
end

mutable struct SubmissionArguments
    h_data::String
    h_rt::String
    email::Bool
end

mutable struct BatchSubmission
    args::SubmissionArguments
    modules::Vector{String}
    code::String
end

function SubmissionArguments()
    return SubmissionArguments(DEFAULT_DATA, DEFAULT_TIME, true)
end

function preamble(sa::SubmissionArguments)
    args = ["cwd" => "",
            "o" => "joblog.out",
            "j" => "y",
            "l" => "h_rt=$(sa.h_rt),h_data=$(sa.h_data),highp"]

    if sa.email
        email_args = ["M" => EMAIL, "m" => "bea"]
        args = [args; email_args]
    end

    hash_bang = "!#/bin/bash"
    lines = ["#\$ -$(p[1]) $(p[2])" for p in args]

    return hash_bang * join(lines, "\n")
end

function make_batch(bs::BatchSubmission)

    batch = preamble(bs.args)
    batch = batch * "\n\n. /u/local/Modules/default/init/modules.sh\n"

    for mod in bs.modules
        batch *= "module load $mod\n"
    end
    batch *= "\n\n$(bs.code)"

    return batch
end

function save_batch(path::String, bs::BatchSubmission)
    save(path, make_batch)
end


function BatchInfo(filenames::Vector{String})
    return BatchInfo(filenames[1],
                    filenames,
                    false,
                    "8G",
                    "300:00:00",
                    "\$HOME/beast-mcmc/build/dist",
                    ".",
                    false,
                    ["$file.savestate" for file in filenames],
                    1_000_000,
                    false,
                    ".",
                    [""],
                    true,
                    false,
                    ".")
end

function BatchInfo(filename::String)
    return BatchInfo([filename])
end


function setup_batch(path::String, bi::BatchInfo)

    filenames = bi.filenames
    multicore = bi.multicore
    data = bi.data
    run_time = bi.run_time
    beast_dir = bi.beast_dir
    source_dir = bi.source_dir
    dest_dir = bi.dest_dir
    save = bi.save
    save_files = bi.save_files
    save_every = bi.save_every
    load = bi.load
    load_dir = bi.load_dir
    load_files = bi.load_files
    email = bi.email
    tmp_dir = bi.tmp_dir

    lines = setup_batch_intro(email, multicore, data, run_time, beast_dir,
        source_dir, dest_dir, tmp_dir)


    for i = 1:length(filenames)
        beast_lines = setup_beast_lines(filenames[i], save, save_files[i],
            save_every, load, load_dir, load_files[i])

        lines = [lines; beast_lines]
    end
    if tmp_dir
        push!(lines, "cp \$TMPDIR/* \$SCRATCH/.")
        push!(lines, "cp \$TMPDIR/* \$DESTDIR/.")
    end

    all_lines = join(lines, "\n")
    write(path, all_lines)
end

function setup_beast_lines(filename::String, save::Bool, save_file::String,
        save_every::Int,
        load::Bool, load_dir::String, load_file::String)
    lines = ["FILENAME=$filename"]
    if save
        push!(lines, "SAVEFILE=$save_file")
        push!(lines, "SAVEEVERY=$save_every")
    end
    if load
        push!(lines, "LOADFILE=$load_dir/$load_file")
    end


    beast_line = "java -Xmx1g -jar -Djava.library.path=\$HOME/lib \$BEASTDIR/beast.jar"
    last_part = "-overwrite \$SOURCEDIR/\$FILENAME.xml > \$HOME/\$FILENAME.txt"
    save_part = "-save_state \$SAVEFILE -save_every \$SAVEEVERY"
    load_part = "-load_state \$LOADFILE"
    if load
        beast_line = "$beast_line $load_part"
    end
    if save
        beast_line = "$beast_line $save_part"
    end
    beast_line = "$beast_line $last_part"
    push!(lines, beast_line)
    return lines
end

function setup_batch_intro(email::Bool, multicore::Bool, data::String,
        run_time::String, beast_dir::String, source_dir::String,
        dest_dir::String, tmp_dir::Bool)
    lines = ["#\$ -cwd"]
    if email
        push!(lines, "#\$ -M gabriel.w.hassler@gmail.com")
        push!(lines, "#\$ -m bea")
    end
    lines = [lines; ["#\$ -o \$HOME/out.joblog",
                    "#\$ -j y"]]
    if multicore
        push!(lines, "#\$ -pe shared 4")
    end
    push!(lines, "#\$ -l h_data=$(data),h_rt=$(run_time),highp")
    lines = [lines;
            ["#!/bin/bash",
            ". /u/local/Modules/default/init/modules.sh",
            "module load java/1.8.0_111",
            "module load gcc/7.2.0",
            "export LD_LIBRARY_PATH=\$HOME/lib:\$LD_LIBRARY_PATH",
            "export PKG_CONFIG_PATH=\$HOME/lib/pkgconfig:\$PKG_CONFIG_PATH",
            "export MALLOC_ARENA_MAX=4",
            "export MALLOC_TRIM_THRESHOLD_=-1"]
            ]
    push!(lines, "BEASTDIR=$beast_dir")
    push!(lines, "SOURCEDIR=$source_dir")
    push!(lines, "DESTDIR=$dest_dir")
    if tmp_dir
        push!(lines, "cd \$TMPDIR")
    else
        push!(lines, "cd \$DESTDIR")
    end
    return lines
end


function setup_sh(dir::String, bis::Vector{BatchInfo})

    n = length(bis)
    batch_names = ["$(bis[i].name).txt" for i = 1:n]

    for i = 1:n
        path = joinpath(dir, batch_names[i])
        setup_batch(path, bis[i])
    end

    qsubs = ["qsub $x" for x in batch_names]
    qsubs = join(qsubs, "\n")
    write(joinpath(dir, "submit.sh"), qsubs)
end

function setup_sh(dir::String, filenames::Array{Array{String, N}, M},
        batch_names::Array{String};
        multicore::Bool = false,
        data::String = "4G",
        run_time::String = "300:00:00",
        beast_dir::String = "\$HOME/beast-mcmc/build/dist",
        source_dir::String = SOURCEDIR,
        dest_dir::String = "\$HOME/Working-Repo/logs",
        save::Bool = false,
        save_files::Array{Array{String, N}, M} = [["$x.savestate" for x in y]
            for y in filenames],
        save_every::Int = 1000000,
        load::Bool = false,
        load_dir::String = dest_dir,
        load_files::Array{Array{String, N}, M} = [["" for x in y] for y in filenames],
        email::Bool = true,
        tmp_dir::Bool = false) where {N, M}

    n = length(filenames)

    for i = 1:n
        path = joinpath(dir, batch_names[i])
        setup_batch(path, filenames[i], multicore = multicore, data = data,
            run_time = run_time, beast_dir = beast_dir, source_dir = source_dir,
            dest_dir = dest_dir, save = save,
            save_files = save_files[i],
            save_every = save_every, load = load, load_dir = load_dir,
            load_files = load_files[i], email = email, tmp_dir = tmp_dir)
    end

    qsubs = ["qsub $x" for x in batch_names]
    qsubs = join(qsubs, "\n")
    write(joinpath(dir, "submit.sh"), qsubs)
end

# function submit_batch(path::String, bi::BatchInfo)
#     setup_batch(path, bi)
#     run(`qsub $path`)
# end



end
