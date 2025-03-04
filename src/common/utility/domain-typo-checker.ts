import freeEmailProviderList from '@/common/utility/free-email-provider-list';

class DomainTypoChecker {
  private array: number[] = [];
  private characterCodeCache: number[] = [];

  public check(inputDomain: string): number | null {
    let closestMatch: string | null = null;
    let smallestDistance: number = Infinity;

    for (const domain of freeEmailProviderList) {
      const distance = this.findDistance(inputDomain, domain);
      if (distance < smallestDistance) {
        smallestDistance = distance;
        closestMatch = domain;
      }
    }

    // Suggest correction only if the typo is within an acceptable range (e.g., distance < 2)
    if (smallestDistance < 2) {
      return smallestDistance;
    }

    return null;
  }

  private findDistance(first: string, second: string): number {
    if (first === second) {
      return 0;
    }

    const swap = first;

    // Swapping the strings if `first` is longer than `second` to ensure shortest comes first
    if (first.length > second.length) {
      first = second;
      second = swap;
    }

    let firstLength = first.length;
    let secondLength = second.length;

    // Performing suffix trimming
    while (
      firstLength > 0 &&
      first.charCodeAt(~-firstLength) === second.charCodeAt(~-secondLength)
    ) {
      firstLength--;
      secondLength--;
    }

    // Performing prefix trimming
    let start = 0;
    while (
      start < firstLength &&
      first.charCodeAt(start) === second.charCodeAt(start)
    ) {
      start++;
    }

    firstLength -= start;
    secondLength -= start;

    if (firstLength === 0) {
      return secondLength;
    }

    let bCharacterCode: number;
    let result: number;
    let temporary: number;
    let temporary2: number;
    let index = 0;
    let index2 = 0;

    while (index < firstLength) {
      this.characterCodeCache[index] = first.charCodeAt(start + index);
      this.array[index] = ++index;
    }

    while (index2 < secondLength) {
      bCharacterCode = second.charCodeAt(start + index2);
      temporary = index2++;
      result = index2;

      for (index = 0; index < firstLength; index++) {
        temporary2 =
          bCharacterCode === this.characterCodeCache[index]
            ? temporary
            : temporary + 1;
        temporary = this.array[index];
        result = this.array[index] =
          temporary > result
            ? temporary2 > result
              ? result + 1
              : temporary2
            : temporary2 > temporary
              ? temporary + 1
              : temporary2;
      }
    }

    return result;
  }
}

export default DomainTypoChecker;
