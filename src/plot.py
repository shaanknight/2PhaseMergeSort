import matplotlib.pyplot as plt
x = [25, 100, 250, 500]
y = [25.383, 27.470, 28.344, 27.931]
plt.plot(x,y, marker='o')
plt.xlabel("Main Mempry(In MB)")
plt.ylabel("Execution Time(In Seconds)")
plt.show()