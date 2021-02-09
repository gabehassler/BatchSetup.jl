module BatchSetup

export setup_batch, setup_sh, BatchInfo, SubmissionArguments



const SOURCEDIR = "\$HOME/Working-Repo/xml"
const DEFAULT_DATA = "8G"
const DEFAULT_TIME = "300:00:00"
const EMAIL = "gabriel.w.hassler@gmail.com"
const DEFAULT_BEAST_DIR = "\$HOME/beast-mcmc/build/dist"

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

function SubmissionArguments()
    return SubmissionArguments(DEFAULT_DATA, DEFAULT_TIME, true)
end

mutable struct XMLInfo
    filename::String
    base_directory::String
    source_directory::String
    dest_directory::String
    save::Bool
    save_path::String
    save_frequency::Int
    load::Bool
    load_path::String
    beast_dir::String

    function XMLInfo(;filename::String="",
                    base_directory::String=ENV["HOME"],
                    source_directory::String=base_directory,
                    dest_directory::String=source_directory,
                    save::Bool=false,
                    save_path::String="",
                    save_frequency::Int=1_000_000,
                    load::Bool=false,
                    load_path::String="",
                    beast_dir=DEFAULT_BEAST_DIR)
        return new(filename,
                    base_directory,
                    source_directory,
                    dest_directory,
                    save,
                    save_path,
                    save_frequency,
                    load,
                    load_path,
                    beast_dir)
    end
end

function XMLInfo(xml_path::String)
    bn = basename(xml_path)
    filename = split(bn, '.')[1]
    src_dir = dirname(xml_path)

    return XMLInfo(filename=filename, source_directory=source_directory)
end

mutable struct BatchSubmission
    id::String
    args::SubmissionArguments
    # modules::Vector{String}
    instructions::Vector{XMLInfo}
end

function BatchSubmission(id::String, args::SubmissionArguments, instructions::XMLInfo)
    return BatchSubmission(id, args, [instructions])
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

    hash_bang = "#!/bin/bash\n"
    lines = ["#\$ -$(p[1]) $(p[2])" for p in args]

    return hash_bang * join(lines, "\n")
end

function make_batch(bs::BatchSubmission)

    batch = preamble(bs.args)
    batch = batch * "\n\n. /u/local/Modules/default/init/modules.sh\n"

    # for mod in bs.modules
    #     batch *= "module load $mod\n"
    # end
    for i in bs.instructions
        batch *= "\n\n$(make_instructions(i))"
    end

    return batch
end

function save_batch(path::String, bs::BatchSubmission)
    write(path, make_batch(bs))
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



function make_instructions(xi::XMLInfo)

    dest_dir = xi.dest_directory
    base_dir = xi.base_directory

    if startswith(dest_dir, base_dir)
        start = length(base_dir) + 2
        dest_dir = dest_dir[start:end]
    end

    lines = [
            "module load java/1.8.0_111",
            "module load gcc/7.2.0",
            "",
            "export LD_LIBRARY_PATH=\$HOME/lib:\$LD_LIBRARY_PATH",
            "export PKG_CONFIG_PATH=\$HOME/lib/pkgconfig:\$PKG_CONFIG_PATH",
            "export MALLOC_ARENA_MAX=4",
            "export MALLOC_TRIM_THRESHOLD_=-1",
            "",
            "BEASTDIR=$(xi.beast_dir)",
            "FILENAME=$(xi.filename)",
            "BASEDIR=$(xi.base_directory)",
            "SOURCEDIR=$(xi.source_directory)",
            "DESTDIR=$(dest_dir)",
            "",
            "cd \$TMPDIR",
            ""]

    if xi.save
        push!(lines, "SAVEFILE=\$BASEDIR/$(xi.save_path)")
        push!(lines, "SAVEEVERY=$(xi.save_frequency)")
    end
    if xi.load
        push!(lines, "LOADFILE=\$BASEDIR/$(xi.load_path)")
    end


    beast_line = "java -Xmx1g -jar -Djava.library.path=\$HOME/lib \$BEASTDIR/beast.jar"
    last_part = "-overwrite \$SOURCEDIR/\$FILENAME.xml > \$HOME/\$FILENAME.txt"

    if xi.load
        load_part = "-load_state \$LOADFILE"
        beast_line = "$beast_line $load_part"
    end
    if xi.save
        save_part = "-save_state \$SAVEFILE -save_every \$SAVEEVERY"
        beast_line = "$beast_line $save_part"
    end
    beast_line = "$beast_line $last_part"
    push!(lines, beast_line)

    lines = [lines;
            ["mkdir -p \$SCRATCH/\$DESTDIR",
             "cp \$TMPDIR/* \$SCRATCH/\$DESTDIR",
             "cp \$TMPDIR/* \$BASEDIR/\$DESTDIR"]
            ]
    return join(lines, '\n')
end


# function setup_batch_intro(email::Bool, multicore::Bool, data::String,
#         run_time::String, beast_dir::String, source_dir::String,
#         dest_dir::String, tmp_dir::Bool)
#     lines = ["#\$ -cwd"]
#     if email
#         push!(lines, "#\$ -M gabriel.w.hassler@gmail.com")
#         push!(lines, "#\$ -m bea")
#     end
#     lines = [lines; ["#\$ -o \$HOME/out.joblog",
#                     "#\$ -j y"]]
#     if multicore
#         push!(lines, "#\$ -pe shared 4")
#     end
#     push!(lines, "#\$ -l h_data=$(data),h_rt=$(run_time),highp")
#     lines = [lines;
#             ["#!/bin/bash",
#             ". /u/local/Modules/default/init/modules.sh",
#             "module load java/1.8.0_111",
#             "module load gcc/7.2.0",
#             "export LD_LIBRARY_PATH=\$HOME/lib:\$LD_LIBRARY_PATH",
#             "export PKG_CONFIG_PATH=\$HOME/lib/pkgconfig:\$PKG_CONFIG_PATH",
#             "export MALLOC_ARENA_MAX=4",
#             "export MALLOC_TRIM_THRESHOLD_=-1"]
#             ]
#     push!(lines, "BEASTDIR=$beast_dir")
#     # push!(lines, "SOURCEDIR=$source_dir")
#     # push!(lines, "DESTDIR=$dest_dir")
#     if tmp_dir
#         push!(lines, "cd \$TMPDIR")
#     else
#         # push!(lines, "cd \$DESTDIR")
#     end
#     return lines
# end


# function setup_sh(dir::String, bis::Vector{BatchInfo})

#     n = length(bis)
#     batch_names = ["$(bis[i].name).txt" for i = 1:n]

#     for i = 1:n
#         path = joinpath(dir, batch_names[i])
#         setup_batch(path, bis[i])
#     end

#     qsubs = ["qsub $x" for x in batch_names]
#     qsubs = join(qsubs, "\n")
#     write(joinpath(dir, "submit.sh"), qsubs)
# end

# function setup_sh(dir::String, filenames::Array{Array{String, N}, M},
#         batch_names::Array{String};
#         multicore::Bool = false,
#         data::String = "4G",
#         run_time::String = "300:00:00",
#         beast_dir::String = "\$HOME/beast-mcmc/build/dist",
#         source_dir::String = SOURCEDIR,
#         dest_dir::String = "\$HOME/Working-Repo/logs",
#         save::Bool = false,
#         save_files::Array{Array{String, N}, M} = [["$x.savestate" for x in y]
#             for y in filenames],
#         save_every::Int = 1000000,
#         load::Bool = false,
#         load_dir::String = dest_dir,
#         load_files::Array{Array{String, N}, M} = [["" for x in y] for y in filenames],
#         email::Bool = true,
#         tmp_dir::Bool = false) where {N, M}

#     n = length(filenames)

#     for i = 1:n
#         path = joinpath(dir, batch_names[i])
#         setup_batch(path, filenames[i], multicore = multicore, data = data,
#             run_time = run_time, beast_dir = beast_dir, source_dir = source_dir,
#             dest_dir = dest_dir, save = save,
#             save_files = save_files[i],
#             save_every = save_every, load = load, load_dir = load_dir,
#             load_files = load_files[i], email = email, tmp_dir = tmp_dir)
#     end

#     qsubs = ["qsub $x" for x in batch_names]
#     qsubs = join(qsubs, "\n")
#     write(joinpath(dir, "submit.sh"), qsubs)
# end

function setup_sh(path::String, subs::Array{BatchSubmission})
    n = length(subs)
    lines = fill("", n)
    dir = dirname(path)
    for i = 1:n
        sub = subs[i]
        batch_path = joinpath(dir, sub.id * ".job")
        save_batch(batch_path, sub)
        lines[i] = "qsub $(batch_path)"
    end

    write(path, join(lines, '\n'))
    return path
end

function setup_sh(path::String, sub::BatchSubmission)
    return setup_sh(path, [sub])
end

function setup_sh(path::String, dir::String;
                  sub_args::SubmissionArguments = SubmissionArguments()
                 )

    ids = String[]
    absolute_dir = abspath(dir)
    submissions = BatchSubmission[]

    for d in walkdir(absolute_dir)
        files = d[3]
        file_dir = d[1]
        for file in files
            if endswith(file, ".xml")
                filename = file[1:(end - 4)]
                r_string = "^$filename(\\d*)\$"
                match_inds = findall(x -> occursin(r_string, x), ids)
                id = filename
                if !isempty(match_inds)
                    max_ind = 0
                    for ind in match_inds
                        m = match(r_string)[1]
                        current_ind = 0
                        if m != ""
                            current_ind = parse(Int, m)
                        end

                        if current_ind > max_ind
                            max_ind = current_ind
                        end
                    end

                    max_ind += 1
                    id = filename * string(max_ind)
                end
                push!(ids, id)
                xi = XMLInfo(filename=filename, source_directory=file_dir)
                bs = BatchSubmission(id, sub_args, xi)
                push!(submissions, bs)
            end
        end
    end

    setup_sh(path, submissions)
end


end
