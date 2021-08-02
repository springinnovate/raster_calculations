"""Tracer cide fir PySCIPOpt."""

from pyscipopt import Model
from pyscipopt import quicksum
import numpy


def main():
    """Entry point."""
    print('build problem')
    dims = 2000
    N = dims*dims
    model = Model()
    x_list = [model.addVar(vtype='B') for _ in range(N)]
    y_list = [float(i) for i in range(N)]

    model.addCons(quicksum(x_list) <= int(N*.1))
    model.setObjective(
        quicksum(x_list[i]*y_list[i] for i in range(N)), "maximize")
    print('start optimize')
    model.optimize()
    sol = model.getBestSol()
    print('solved')
    print(sum(sol[x_list[i]]*y_list[i] for i in range(N)))
    # model.setObjective(x)
    # model.addCons(2*x - y*y >= 0)
    # model.optimize()
    # sol = model.getBestSol()
    # print("x: {}".format(sol[x]))
    # print("y: {}".format(sol[y]))

    # x = model.addVar("x")
    # y = model.addVar("y", vtype="INTEGER")
    # model.setObjective(x + y)
    # model.addCons(2*x - y*y >= 0)
    # model.optimize()
    # sol = model.getBestSol()
    # print("x: {}".format(sol[x]))
    # print("y: {}".format(sol[y]))


if __name__ == '__main__':
    main()
