#include <iostream>
#include <string>

int main() {
    std::ios_base::sync_with_stdio(false);
    std::cin.tie(0);
    std::string a, r, b, pa, pr, pb;
    long long int linei = 0;
    while (std::cin >> a >> r >> b) {
        if (a == pa && b == pb) {
            if (r != "R" && pr != "C") {
                std::cerr << "Line " << linei << " error.\n";
            }
            continue;
        }
        std::cout << a << '\t' << r << '\t' << b << '\n';
        pa = a;
        pb = b;
        pr = r;
        ++linei;

    }
    return 0;
}
