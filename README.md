# MapReduce implementation in Go


Hello! As the title suggests, this is an implementation of the MapReduce algorithm in Go. I worked on this project as part of my Distributed Computing course. The project itself is part of [MIT's 6.824 course from Spring 2020](http://nil.csail.mit.edu/6.824/2020/labs/lab-mr.html).

I obtained the starter code from one of the GitHub repositories and developed this implementation to be as faithful as possible to the one described in the [MapReduce Paper](https://static.googleusercontent.com/media/research.google.com/en//archive/mapreduce-osdi04.pdf).

Similar to the 2020 lab, there are tests are present in the `cmd/scripts` that rigoursly test the implementation under different normal and abnormal circumstances (such as master failure, unresponsive node, etc.)

To run the battery of tests, you can `cd cmd/scripts && bash test-mr.sh`.

Thanks!