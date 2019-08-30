from intern.remote.boss import BossRemote
from intern.resource.boss.resource import *

# admin token needed to list all projects
rmt = BossRemote("/home/ben/Documents/travis_user_neurodata.cfg")

colls = rmt.list_collections()

with open("public_datasets.csv", "w") as f:
    f.write(
        "coll,exp,ch,exp_description,num_hierarchy_levels,dtype,x_start,x_stop,y_start,y_stop,z_start,z_stop\n"
    )

for coll in colls:
    exps = rmt.list_experiments(coll)
    for exp in exps:
        exp_res = rmt.get_experiment(coll, exp)
        coord_frame_res = rmt.get_coordinate_frame(exp_res.coord_frame)

        chs = rmt.list_channels(coll, exp)
        for ch in chs:
            ch_res = rmt.get_channel(ch, coll, exp)
            with open("public_datasets.csv", "a") as f:
                f.write(
                    f"{coll},{exp},{ch},{exp_res.description},{exp_res.num_hierarchy_levels},{ch_res.datatype},{coord_frame_res.x_start},{coord_frame_res.x_stop},{coord_frame_res.y_start},{coord_frame_res.y_stop},{coord_frame_res.z_start},{coord_frame_res.z_stop}\n"
                )
