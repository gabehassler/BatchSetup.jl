module BatchSetup

export setup_batch, setup_sh, SubmissionArguments, setup_sh_depth



const SOURCEDIR = "\$HOME/Working-Repo/xml"
const DEFAULT_DATA = "8G"
const DEFAULT_TIME = "300:00:00"
const EMAIL = "gabriel.w.hassler@gmail.com"
const DEFAULT_BEAST_DIR = "\$HOME/beast-mcmc/build/dist"


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
    batch *= join([make_instructions(xi) for xi in bs.instructions],
                    "\nrm \$TMPDIR/*\n\n")
    for i in bs.instructions
        batch *= "\n\n$(make_instructions(i))"
    end

    return batch
end

function save_batch(path::String, bs::BatchSubmission)
    write(path, make_batch(bs))
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

function setup_sh(path::String, subs::Array{BatchSubmission})
    n = length(subs)
    lines = fill("", n)
    dir = dirname(path)
    ids = [s.id for s in subs]
    if length(unique(ids)) != length(ids)
        error("All batch ids must be unique.")
    end
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

function get_submissions(path::String, dir::String;
                         sub_args::SubmissionArguments = SubmissionArguments(),
                         combined::Bool = false
                        )

    ids = String[]
    xmls = XMLInfo[]
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
                push!(xmls, xi)
                # bs = BatchSubmission(id, sub_args, xi)
                # push!(submissions, bs)
            end
        end
    end

    if combined
        submissions = [BatchSubmission(ids[1] * "_combined", sub_args, xmls)]
    else
        submissions = [BatchSubmission(ids[i], sub_args, xmls[i]) for i = 1:lenght(xis)]
    end

    return submissions
end

function setup_sh(path::String, dir::String;
                  sub_args::SubmissionArguments = SubmissionArguments(),
                  combined::Bool = false)
    submissions = get_submissions(path, dir,
                                  sub_args = sub_args, combined = combined)
    return setup_sh(path, submissions)
end

function setup_sh_depth(path::String, dir::String, depth::Int;
                        sub_args::SubmissionArguments = SubmissionArguments(),
                        all_submissions = BatchSubmission[],
                        original_depth = depth)
    if depth == 0
        sub = get_submissions(path, dir, sub_args = sub_args, combined = true)[1] # there should only be one
        sub.id = splitpath(dir)[end]
        push!(all_submissions, sub)
    else
        for sub_dir in readdir(dir, join=true)
            if isdir(sub_dir)
                setup_sh_depth(path, sub_dir, depth - 1,
                               sub_args = sub_args,
                               all_submissions = all_submissions,
                               original_depth = original_depth)
            end
        end
    end

    if depth == original_depth
        ids = [s.id for s in all_submissions]
        if length(unique(ids)) != length(ids)
            for i = 1:length(all_submissions)
                id = all_submissions[i].id
                all_submissions[i].id = id * "_$i"
            end
        end

        return setup_sh(path, all_submissions)
    end

    return nothing
end


end
